### 0.3.0 / 2025-04-08

* Add SSL support for Redis connections
* Add Redis Sentinel support for high availability
* Replace em-hiredis with native Redis gem and em-synchrony
* Add connection pool configuration
* Add heartbeat mechanism for better connection reliability
* Improve thread safety with mutex-protected shared data


### 0.2.0 / 2013-10-01

* Migrate from Yajl to MultiJson to support JRuby
* Trigger `close` event as required by Faye 1.0


### 0.1.1 / 2013-04-28

* Improve garbage collection to avoid leaking Redis memory


### 0.1.0 / 2012-02-26

* Initial release: Redis backend for Faye 0.8

