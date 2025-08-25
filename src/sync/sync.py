#!/usr/bin/env python3

import sys
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
from datetime import datetime
from collections import defaultdict

# Import debug utilities
try:
    from utils.debug import debug_print_sync
except ImportError:
    def debug_print_sync(msg): pass

# our sync class
class KP_Sync:

    # fire us up
    def __init__( self, max_threads=None ):

        # class imports
        from common.common import KP_Common
        self.common = KP_Common( )

        debug_print_sync("Initializing KP_Sync")

        # Thread pool configuration
        cpu_count = os.cpu_count( ) or 1
        default_threads = min( 8, max( 4, cpu_count * 2 ) )  # Better default range
        self.max_threads = self._determine_thread_count( max_threads, default_threads )
        
        debug_print_sync(f"Using {self.max_threads} threads for sync operations")
        
        # Initialize components
        from utils.cache import KP_Cache
        _cache = KP_Cache( )

        # clear our cache
        debug_print_sync("Clearing cache")
        _cache.clear( )
        del _cache

        # setup the internal sync data
        from sync.data import KP_Sync_Data
        self._data = KP_Sync_Data( )

        # setup the thread locks
        self._thread_lock = threading.Lock( )
        self._db_lock = threading.Lock( )

        debug_print_sync("KP_Sync initialization completed")

    # our main public sync function
    def sync( self ):
        
        debug_print_sync("Starting sync operation")
        
        # get our providers, and make sure we have some before proceeding
        _providers = self._data._get_providers( self.common.args.provider )
        if not _providers:
            self.common.kp_print( "error", "No providers found" )
            sys.exit( 1 )

        debug_print_sync(f"Found {len(_providers)} providers to process")

        # Show initial sync message
        self.common.kp_print_line( )
        self.common.kp_print( "info", "STARTING PROVIDER SYNC" )
        self.common.kp_print( "info", "Providers to process:" )
        for prov in _providers:
            self.common.kp_print( "info", f"- {prov['sp_name']}" )
        self.common.kp_print_line( )

        # hold our start time
        start_time = time.time( )

        # setup the results and error internals
        results = []
        has_errors = False
        
        debug_print_sync("Starting thread pool execution")
        
        # with our thread executor
        with ThreadPoolExecutor( max_workers=self.max_threads ) as executor:

            # setup the executions we're taking
            futures = {executor.submit( self._process_provider, prov ): prov['sp_name'] 
                      for prov in _providers}
            
            debug_print_sync(f"Submitted {len(futures)} provider processing tasks")
            
            # for each completed action: 1 hour timeout
            for future in as_completed( futures, timeout=3600 ):

                res = future.result( )

                if res is not None:

                    # setup the results
                    total, filtered, name, error = res

                    debug_print_sync(f"Provider {name} completed: {filtered}/{total} streams, error: {error}")

                    # oofff... if we have an error
                    if error:

                        # set the flag, and show the error
                        has_errors = True
                        self.common.kp_print( "error", f"Error processing {name}: {error}" )
                    
                    # append our results
                    results.append( ( total, filtered, name, error ) )

        debug_print_sync("Thread pool execution completed")

        # Final operations
        try:

            debug_print_sync("Starting final database operations")

            # utilize the database thread locker
            with self._db_lock:

                # sync the streams
                debug_print_sync("Syncing streams to database")
                self._data._sync_the_streams( )

                # clean up the streams
                debug_print_sync("Cleaning up streams")
                self._data._cleanup( )

                # attempt to fix up some data in the streams
                debug_print_sync("Running fixup operations")
                self.fixup( )

            debug_print_sync("Final database operations completed")

        # yikes, there was an error
        except Exception as e:
            has_errors = True
            self.common.kp_print( "error", f"Final operations failed: {str(e)}" )
            debug_print_sync(f"Final operations error: {e}")

        # Show final summary
        self._print_final_summary( results, time.time( ) - start_time, has_errors )

    # test streams for validity
    def test_streams( self ):
        
        debug_print_sync("Starting stream testing operation")
        
        # get active streams with provider info
        streams = self._data._get_active_streams()
        if not streams:
            self.common.kp_print( "error", "No active streams found" )
            return

        debug_print_sync(f"Found {len(streams)} active streams to test")

        # Group streams by provider for display
        provider_groups = defaultdict(list)
        provider_info = {}
        
        for stream in streams:
            provider_id = stream['p_id']
            provider_groups[provider_id].append(stream)
            
            if provider_id not in provider_info:
                provider_info[provider_id] = {
                    'name': stream['sp_name'],
                    'cnx_limit': stream['sp_cnx_limit'],
                    'stream_count': 0
                }
            
            provider_info[provider_id]['stream_count'] += 1

        # Show initial test message
        self.common.kp_print_line( )
        self.common.kp_print( "info", "STARTING STREAM TESTING" )
        self.common.kp_print( "info", f"Testing {len(streams)} active streams from {len(provider_groups)} providers" )
        
        # Show provider info
        for provider_id, info in provider_info.items():
            self.common.kp_print( "info", f"- {info['name']}: {info['stream_count']} streams (limit: {info['cnx_limit']} connections)" )

        # hold our start time
        start_time = time.time( )

        # Initialize results tracking
        tested_count = 0
        valid_count = 0
        invalid_count = 0
        invalid_streams = []  # For logging
        
        # Create log file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"invalid_streams_{timestamp}.log"
        
        debug_print_sync("Starting thread pool execution for stream testing")
        
        # Use simpler threading approach
        with ThreadPoolExecutor( max_workers=4 ) as executor:

            futures = {executor.submit( self._test_single_stream_simple, stream ): stream['id'] 
                      for stream in streams}
            
            debug_print_sync(f"Submitted {len(futures)} stream testing tasks")
            
            # for each completed test
            for future in as_completed( futures, timeout=7200 ):

                try:
                    stream_data, is_valid, error = future.result( )
                    tested_count += 1

                    if is_valid:
                        valid_count += 1
                        debug_print_sync(f"Stream {stream_data['id']} is valid")
                    else:
                        invalid_count += 1
                        invalid_streams.append((stream_data, error))
                       
                        debug_print_sync(f"Stream {stream_data['id']} is invalid: {error}")

                    # Progress update every 100 streams
                    if tested_count % 100 == 0:
                        progress_msg = f"Progress: {tested_count}/{len(streams)} tested ({valid_count} valid, {invalid_count} invalid)"
                        self.common.kp_print( "info", progress_msg )

                except Exception as e:
                    debug_print_sync(f"Error testing stream: {e}")
                    invalid_count += 1

        debug_print_sync("Thread pool execution completed for stream testing")

        # Write invalid streams to log file
        if invalid_streams:
            try:
                with open(log_filename, 'w', encoding='utf-8') as log_file:
                    log_file.write(f"Invalid Streams Log - Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    log_file.write("=" * 80 + "\n\n")
                    
                    for stream_data, error in invalid_streams:
                        log_file.write(f"ID: {stream_data['id']}\n")
                        log_file.write(f"Name: {stream_data['s_orig_name']}\n")
                        log_file.write(f"URL: {stream_data['s_stream_uri']}\n")
                        log_file.write(f"Provider: {stream_data.get('sp_name', 'Unknown')}\n")
                        log_file.write(f"Error: {error}\n")
                        log_file.write("-" * 40 + "\n")
                        
                debug_print_sync(f"Invalid streams logged to: {log_filename}")
            except Exception as e:
                self.common.kp_print( "error", f"Failed to write log file: {str(e)}" )

    # fix invalid streams from log file
    def fix_from_log( self ):
        
        debug_print_sync("Starting fix from log operation")
        
        # Find the most recent log file
        log_file = self._find_latest_log_file()
        if not log_file:
            self.common.kp_print( "error", "No invalid streams log file found" )
            return
        
        self.common.kp_print_line( )
        self.common.kp_print( "info", "FIXING INVALID STREAMS FROM LOG" )
        self.common.kp_print( "info", f"Using log file: {log_file}" )
        self.common.kp_print_line( )
        
        # Parse the log file to extract stream IDs
        stream_ids = self._parse_log_file(log_file)
        if not stream_ids:
            self.common.kp_print( "error", "No stream IDs found in log file" )
            return
        
        self.common.kp_print( "info", f"Found {len(stream_ids)} invalid streams to move" )
        
        # Move the streams in batch
        start_time = time.time()
        try:
            moved_count = self._data._batch_move_streams_to_other(stream_ids)
            
            # Show final summary
            self.common.kp_print_line( )
            self.common.kp_print( "info", "FIX FROM LOG SUMMARY" )
            self.common.kp_print_line( )
            self.common.kp_print( "info", f"Log file processed: {log_file}" )
            self.common.kp_print( "info", f"Streams to move: {len(stream_ids)}" )
            self.common.kp_print( "info", f"Streams moved: {moved_count}" )
            self.common.kp_print( "info", f"Total time: {time.time() - start_time:.1f} seconds" )
            
            if moved_count == len(stream_ids):
                self.common.kp_print( "success", "ALL INVALID STREAMS MOVED SUCCESSFULLY" )
            else:
                self.common.kp_print( "warn", f"PARTIAL SUCCESS - {moved_count}/{len(stream_ids)} STREAMS MOVED" )
            
            self.common.kp_print_line( )
            
        except Exception as e:
            self.common.kp_print( "error", f"Failed to move streams: {str(e)}" )
            debug_print_sync(f"Fix from log failed: {e}")

    def _find_latest_log_file( self ):
        """Find the most recent invalid_streams_*.log file"""
        import glob
        import os
        
        # Look for log files in current directory
        log_pattern = "invalid_streams_*.log"
        log_files = glob.glob(log_pattern)
        
        if not log_files:
            debug_print_sync("No log files found matching pattern: " + log_pattern)
            return None
        
        # Sort by modification time (newest first)
        log_files.sort(key=os.path.getmtime, reverse=True)
        latest_log = log_files[0]
        
        debug_print_sync(f"Found {len(log_files)} log files, using latest: {latest_log}")
        return latest_log

    def _parse_log_file( self, log_file ):
        """Parse log file to extract stream IDs"""
        stream_ids = []
        
        debug_print_sync(f"Parsing log file: {log_file}")
        
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for line in lines:
                line = line.strip()
                # Look for lines that start with "ID: "
                if line.startswith("ID: "):
                    try:
                        stream_id = int(line[4:])  # Extract ID after "ID: "
                        stream_ids.append(stream_id)
                        debug_print_sync(f"Extracted stream ID: {stream_id}")
                    except ValueError:
                        debug_print_sync(f"Could not parse stream ID from line: {line}")
                        continue

            debug_print_sync("Running cleanup operations")

            # run it
            self._data._cleanup( )
            
            debug_print_sync(f"Parsed {len(stream_ids)} stream IDs from log file")
            return stream_ids
            
        except Exception as e:
            debug_print_sync(f"Error parsing log file {log_file}: {e}")
            return []
        

    # fixup the data
    def fixup( self ):

        debug_print_sync("Running fixup operations")
        
        # run it
        self._data._fixup( )

    # determin the thread optimal thread count
    def _determine_thread_count( self, max_threads, default ):

        # if we do not have something configured
        if max_threads is not None:

            # return a default
            thread_count = max( 1, min( int( max_threads ), 16 ) )
            debug_print_sync(f"Using configured thread count: {thread_count}")
            return thread_count
        
        # otherwise, return the set default
        debug_print_sync(f"Using default thread count: {default}")
        return default

    # test a single stream
    def _test_single_stream( self, stream_data, provider_semaphores ):
        
        debug_print_sync(f"Testing stream: {stream_data['id']}")
        
        try:
            from sync.test import KP_StreamTester
            tester = KP_StreamTester()
            
            # Set provider semaphores in the tester
            tester.set_provider_semaphores(provider_semaphores)
            
            is_valid, error = tester.test_stream(stream_data)
            
            return stream_data, is_valid, error
            
        except Exception as e:
            debug_print_sync(f"Error testing stream {stream_data['id']}: {e}")
            return stream_data, False, f"Testing error: {str(e)}"

    # Simple test method without semaphores (fallback)
    def _test_single_stream_simple( self, stream_data ):
        
        debug_print_sync(f"Testing stream (simple): {stream_data['id']}")
        
        try:
            from sync.test import KP_StreamTester
            tester = KP_StreamTester()
            
            is_valid, error = tester.test_stream(stream_data)
            
            return stream_data, is_valid, error
            
        except Exception as e:
            debug_print_sync(f"Error testing stream {stream_data['id']}: {e}")
            return stream_data, False, f"Testing error: {str(e)}"

    # process a provider
    def _process_provider( self, _prov ):

        debug_print_sync(f"Processing provider: {_prov['sp_name']}")

        # try to process
        try:

            # with our database thread lockers
            with self._db_lock:

                debug_print_sync(f"Getting filters for provider {_prov['sp_name']}")
                # try to grab the users filters
                _filters = self._data._get_filters( _prov["u_id"] )
                if _filters is None:
                    debug_print_sync(f"No filters found for provider {_prov['sp_name']}")
                    return ( 0, 0, _prov['sp_name'], "No filters found" )

                debug_print_sync(f"Found {len(_filters)} filters for provider {_prov['sp_name']}")

            # Get and process streams
            from sync.get import KP_Get
            _get = KP_Get( )

            debug_print_sync(f"Fetching streams for provider {_prov['sp_name']}")
            # get the streams from the provider
            _streams = _get.get_streams( _prov )
            
            debug_print_sync(f"Retrieved {len(_streams)} streams for {_prov['sp_name']}")
            
            # setup the filtering
            from sync.filter import KP_Filter

            debug_print_sync(f"Applying filters to streams for {_prov['sp_name']}")
            # filter the streams
            _filtered_streams = KP_Filter.filter_streams( _streams, _filters )

            debug_print_sync(f"Filtered to {len(_filtered_streams)} streams for {_prov['sp_name']}")

            # now convert them to our common format
            _converted_streams = self._convert_streams( _filtered_streams, _prov )
            
            debug_print_sync(f"Converted {len(_converted_streams)} streams for {_prov['sp_name']}")
            
            # make sure we actually have converted streams
            if _converted_streams:

                # with out database lock
                with self._db_lock:

                    debug_print_sync(f"Inserting streams to database for {_prov['sp_name']}")
                    # insert the streams
                    self._data._insert_the_streams( _converted_streams )

                    debug_print_sync(f"Updating last synced time for {_prov['sp_name']}")
                    # update the last synced
                    self._data._update_last_synced( _prov["u_id"] )
            
            debug_print_sync(f"Provider {_prov['sp_name']} processing completed successfully")
            # return the streams
            return ( len( _streams ), len( _converted_streams ), _prov['sp_name'], None )
            
        # whoops... 
        except Exception as e:
            debug_print_sync(f"Provider {_prov['sp_name']} processing failed: {e}")
            return ( 0, 0, _prov['sp_name'], str( e ) )

    # setup and format the final "report"
    def _print_final_summary( self, results, total_time, has_errors ):

        # THIS IS THE ONLY OUTPUT THAT SHOULD SHOW WITHOUT --debug
        # Keep this visible for users
        
        self.common.kp_print_line( )
        self.common.kp_print("info", "SYNC SUMMARY")
        self.common.kp_print_line( )
        
        # how many were sucessful
        successful = [r for r in results if not r[3]]

        # how many faild
        failed = [r for r in results if r[3]]
        
        # if we have successes
        if successful:
            self.common.kp_print( "info", "SUCCESSFUL PROVIDERS:" )

            # loop the successes
            for total, filtered, name, _ in successful:

                # print out what the were with the stats
                self.common.kp_print( "info", f"- {name}: {total}/{filtered} streams" )
        
        # if we 
        if failed:
            self.common.kp_print( "info", "FAILED PROVIDERS:" )

            # loop the failures
            for _, _, name, error in failed:

                # print em out
                self.common.kp_print( "info", f"- {name} ({error})" )
        
        self.common.kp_print( "info", "\nSTATISTICS:" )
        self.common.kp_print( "info", f"Total providers: {len(results)}" )
        self.common.kp_print( "info", f"Successful: {len(successful)}" )
        self.common.kp_print( "info", f"Failed: {len(failed)}" )
        self.common.kp_print( "info", f"Total time: {total_time:.1f} seconds" )
        
        # if we had errors
        if has_errors:
            self.common.kp_print( "warn", "SYNC COMPLETED WITH ERRORS" )

        # otherwise
        else:
            self.common.kp_print( "success", "SYNC COMPLETED SUCCESSFULLY" )
            
        self.common.kp_print_line( )

    # setup and format the test summary
    def _print_test_summary( self, tested_count, valid_count, invalid_count, moved_count, total_time, log_filename, fix_mode ):
        
        self.common.kp_print_line( )
        self.common.kp_print("info", "STREAM TESTING SUMMARY")
        self.common.kp_print_line( )
        
        self.common.kp_print( "info", "STATISTICS:" )
        self.common.kp_print( "info", f"Total streams tested: {tested_count}" )
        self.common.kp_print( "info", f"Valid streams: {valid_count}" )
        self.common.kp_print( "info", f"Invalid streams: {invalid_count}" )
        
        if fix_mode:
            self.common.kp_print( "info", f"Streams moved to other table: {moved_count}" )
        
        if tested_count > 0:
            validity_percentage = (valid_count / tested_count) * 100
            self.common.kp_print( "info", f"Validity rate: {validity_percentage:.1f}%" )
        
        self.common.kp_print( "info", f"Total time: {total_time:.1f} seconds" )
        
        if log_filename:
            self.common.kp_print( "info", f"Invalid streams logged to: {log_filename}" )
        
        if invalid_count == 0:
            self.common.kp_print( "success", "ALL STREAMS ARE VALID" )
        else:
            status_msg = f"TESTING COMPLETED - {invalid_count} INVALID STREAMS FOUND"
            if fix_mode:
                status_msg += f" AND {moved_count} MOVED"
            self.common.kp_print( "warn", status_msg )
            
        self.common.kp_print_line( )

    # convert our streams to a standardized format
    def _convert_streams( self, streams, provider ):

        debug_print_sync(f"Converting {len(streams)} streams for provider {provider['sp_name']}")

        # return the formatted streams        
        converted = [{
            'u_id': provider['u_id'],
            'p_id': provider['id'],
            's_orig_name': stream['stream_name'],
            's_stream_uri': stream['stream_url'],
            's_type_id': stream['stream_type'],
            's_tvg_id': stream['epg_id'],
            's_tvg_logo': stream['stream_icon'],
            's_extras': '',
            's_group': stream['stream_group'],
        } for _, stream in streams.items( )]
        
        debug_print_sync(f"Converted {len(converted)} streams successfully")
        return converted