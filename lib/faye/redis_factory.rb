require 'redis'
require 'em-synchrony'
require 'em-synchrony/connection_pool'
require 'thread'

module Faye
  class RedisFactory
    DEFAULT_PORT     = 6379
    DEFAULT_DATABASE = 0
    DEFAULT_POOL_SIZE = 5

    def initialize(options)
      @options = options
    end

    def call
      uri         = @options[:uri]          || nil
      socket      = @options[:socket]       || nil
      sentinels   = @options[:sentinels]    || nil
      master_name = @options[:master_name]  || nil
      host        = @options[:host]         || nil
      port        = @options[:port]         || DEFAULT_PORT
      auth        = @options[:password]     || nil
      db          = @options[:database]     || DEFAULT_DATABASE
      ssl_params  = @options[:ssl_params]   || {}
      pool_size   = @options[:pool_size]    || DEFAULT_POOL_SIZE
      
      # Determine connection options based on configuration
      redis_options = if uri
        # Use URI with SSL
        { url: uri, ssl: true, ssl_params: ssl_params }
      elsif socket
        # Unix socket doesn't use SSL
        { path: socket, password: auth, db: db }
      elsif sentinels && master_name
        # Sentinel configuration with SSL
        {
          sentinels: sentinels,
          name: master_name,
          role: :master,
          password: auth,
          db: db,
          ssl: true,
          ssl_params: ssl_params
        }
      elsif host
        # Direct connection with SSL
        {
          host: host,
          port: port,
          password: auth, 
          db: db,
          ssl: true,
          ssl_params: ssl_params
        }
      else
        raise ArgumentError, "Redis connection requires either :uri, :socket, :host, or :sentinels/:master_name"
      end
      
      # Add pool size to options
      redis_options[:pool_size] = pool_size
      
      EMSynchronyRedisAdapter.new(redis_options)
    end
  end
  
  # Adapter to make Redis work with EventMachine
  class EMSynchronyRedisAdapter
    def initialize(options)
      @options = options
      pool_size = options[:pool_size] || RedisFactory::DEFAULT_POOL_SIZE
      
      # Create a connection pool for Redis operations
      @pool = EM::Synchrony::ConnectionPool.new(size: pool_size) do
        Redis.new(options)
      end
      @pubsub = nil
    end
    
    def pubsub
      @pubsub ||= EMSynchronyRedisPubSubAdapter.new(@options)
    end
    
    # Methods that need to pass through to Redis and return deferrables
    def method_missing(method, *args, &callback)
      # Create a deferrable for EM compatibility
      deferrable = EventMachine::DefaultDeferrable.new
      
      # Execute Redis operation in a non-blocking way using EM::Synchrony
      EM::Synchrony.next_tick do
        begin
          # Execute the Redis command
          result = @pool.execute do |redis|
            redis.send(method, *args)
          end
          
          # Succeed the deferrable with the result
          deferrable.succeed(result)
          
          # Call the callback if provided
          callback.call(result) if callback
        rescue => e
          # Fail the deferrable with the error
          deferrable.fail(e)
        end
      end
      
      deferrable
    end
    
    # Add a connected? method to check connection status
    def connected?
      begin
        @pool.execute { |redis| redis.ping == "PONG" }
      rescue => e
        false
      end
    end
  end
  
  # PubSub adapter for Redis with EM::Synchrony
  class EMSynchronyRedisPubSubAdapter
    HEARTBEAT_INTERVAL = 30  # Seconds between heartbeats
    
    def initialize(options)
      @options = options
      @redis = Redis.new(options)
      @message_callbacks = []
      @subscriptions = {}
      @mutex = Mutex.new  # Add mutex for thread safety
      @subscribed = false
      @restart_count = 0
      
      # Start the heartbeat timer to detect disconnections
      start_heartbeat
      
      # Start the message listener in a fiber
      start_listener
    end
    
    def subscribe(channel)
      # Create a deferrable for EM compatibility
      deferrable = EventMachine::DefaultDeferrable.new
      
      # Thread-safe access to subscriptions
      @mutex.synchronize do
        @subscriptions[channel] = true
      end
      
      # Immediately succeed since the actual subscribe happens in the listener fiber
      EM::Synchrony.next_tick do
        deferrable.succeed
      end
      
      deferrable
    end
    
    def unsubscribe(channel)
      # Create a deferrable for EM compatibility
      deferrable = EventMachine::DefaultDeferrable.new
      
      # Thread-safe access to subscriptions
      @mutex.synchronize do
        @subscriptions.delete(channel)
      end
      
      # The actual unsubscribe happens in the listener fiber
      if @subscribed
        begin
          @redis.unsubscribe(channel)
        rescue => e
          puts "Error unsubscribing from #{channel}: #{e.message}"
        end
      end
      
      EM::Synchrony.next_tick do
        deferrable.succeed
      end
      
      deferrable
    end
    
    def on(event_type, &callback)
      if event_type == :message
        @mutex.synchronize do
          @message_callbacks << callback
        end
      end
    end
    
    # Clean up resources
    def shutdown
      cancel_heartbeat
      @redis.quit rescue nil
    end
    
    private
    
    def start_heartbeat
      @heartbeat_timer = EM.add_periodic_timer(HEARTBEAT_INTERVAL) do
        check_connection
      end
    end
    
    def cancel_heartbeat
      EM.cancel_timer(@heartbeat_timer) if @heartbeat_timer
      @heartbeat_timer = nil
    end
    
    def check_connection
      # Skip if we're already in a subscribe block
      return if @subscribed
      
      # Perform a PING to check if connection is alive
      begin
        @redis.ping
        # Ping succeeded, reset restart count
        @restart_count = 0
      rescue => e
        puts "Redis connection lost during heartbeat: #{e.message}"
        reconnect_redis
      end
    end
    
    def reconnect_redis
      # Reconnect the Redis instance
      begin
        @redis.quit rescue nil
        @redis = Redis.new(@options)
        # Restart the listener since we have a new connection
        restart_listener
      rescue => e
        puts "Failed to reconnect to Redis: #{e.message}"
        
        # Add backoff for reconnection attempts
        @restart_count += 1
        if @restart_count < 10
          backoff_time = [0.1 * (2 ** @restart_count), 30].min
          EM::Synchrony.sleep(backoff_time)
          reconnect_redis
        else
          puts "Too many Redis reconnection attempts, giving up"
        end
      end
    end
    
    def restart_listener
      @subscribed = false
      start_listener
    end
    
    def start_listener
      # Start a separate fiber for the pubsub connection
      EM::Synchrony.fiber do
        begin
          # Thread-safe access to get channels
          channels = nil
          @mutex.synchronize do
            channels = @subscriptions.keys
          end
          
          # Only subscribe if we have channels to listen to
          if channels.any?
            @subscribed = true
            
            # Use the Redis blocking subscribe in a fiber
            @redis.subscribe(*channels) do |on|
              on.message do |channel, message|
                # Create a local copy of callbacks under mutex protection
                callbacks = nil
                @mutex.synchronize do
                  callbacks = @message_callbacks.dup
                end
                
                # Use the local copy outside the mutex
                callbacks.each do |callback|
                  EM::Synchrony.next_tick do
                    callback.call(channel, message)
                  end
                end
              end
              
              on.subscribe do |channel, count|
                # Thread-safe update of subscription state
                @mutex.synchronize do
                  @subscriptions[channel] = true
                end
              end
              
              on.unsubscribe do |channel, count|
                # If no more subscriptions, exit the subscribe block
                if count == 0
                  @subscribed = false
                end
              end
            end
          end
        rescue => e
          # Log the error and restart the listener with backoff
          puts "Error in Redis PubSub listener: #{e.message}"
          
          # Add retry limits to prevent infinite loops
          @restart_count ||= 0
          @restart_count += 1
          
          if @restart_count < 10  # Limit retries to 10 attempts
            # Use exponential backoff to avoid rapid reconnection attempts
            backoff_time = [0.1 * (2 ** @restart_count), 30].min  # Max 30 seconds
            EM::Synchrony.sleep(backoff_time)
            start_listener
          else
            puts "Too many Redis PubSub reconnection attempts, giving up"
          end
        end
      end
    end
  end
end
