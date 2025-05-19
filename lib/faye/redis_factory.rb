module Faye
  class RedisFactory

    DEFAULT_HOST     = '0.0.0.0'
    DEFAULT_PORT     = 6379
    DEFAULT_DATABASE = 0

    def initialize(options)
      @options = options
      @ssl_params = @options[:ssl_params]

      unless @ssl_params && @ssl_params[:ca_file]
        raise ArgumentError, 'Missing required SSL parameter: ssl_params must contain a ca_file.'
      end

      # Ensure verify_peer is present, defaulting to true if not specified.
      @ssl_params[:verify_peer] = true unless @ssl_params.key?(:verify_peer)
    end

    def call
      host   = @options[:host]     || DEFAULT_HOST
      port   = @options[:port]     || DEFAULT_PORT
      auth   = @options[:password] || nil
      db     = @options[:database] || DEFAULT_DATABASE

      EventMachine::Hiredis::Client.new(host, port, auth, db, ssl_options: @ssl_params).connect
    end

  end
end
