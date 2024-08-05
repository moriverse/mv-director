import os
import socket
import structlog
import time

from kombu import Connection, Queue

log = structlog.get_logger(__name__)

class RedisConsumer:
    
    def consume(self, redis_url, queue, on_message, on_pre_message=None, aborted=None, timeout=30):
        log.info(f"Connecting to redis queue: {queue} with timeout {timeout}")
        
        with Connection(redis_url) as conn:
            # Auto failover - drain every second to avoid broken connection.
            # Consumer will break on should_exit or idle for timeout interval.
            def consume():
                mark = time.perf_counter()
                while True:
                    try:
                        if on_pre_message is not None: 
                            on_pre_message()
                        
                        conn.drain_events(timeout=1)
                        
                        # Update mark after message handled.
                        mark = time.perf_counter()
                        
                    except socket.timeout:
                        pass
                    
                    is_timeout = timeout > 0 and time.perf_counter() - mark >= timeout
                    is_aborted = aborted is not None and aborted()
                    if is_timeout or is_aborted:
                        log.warn(
                            "Consumer exiting.", 
                            is_timeout=is_timeout, 
                            is_aborted=is_aborted
                        )
                        break
                
            with conn.Consumer(
                Queue(queue, routing_key=queue), 
                callbacks=[on_message], 
                prefetch_count=1
            ):
                try:
                    conn.ensure(conn, consume)()
                    
                except Exception as e:
                    log.error("Exception on consumer.", error=e)