#!/usr/bin/env python3

# import the database class
from db.db import KP_DB, ComparisonOperator, WhereClause

# Import debug utilities
try:
    from utils.debug import debug_print_sync, debug_print_db
except ImportError:
    def debug_print_sync(msg): pass
    def debug_print_db(msg): pass

class KP_Sync_Data:

    def __init__( self ):

        debug_print_sync("Initializing KP_Sync_Data")

        # setup the cache we're going to use
        from utils.cache import KP_Cache
        self.cache = KP_Cache( )

        # hold the cache keys
        self.cache_key_prov = "sync_providers"
        self.cache_key_filt = "sync_filters_%d"
        
        debug_print_sync("KP_Sync_Data initialization completed")

    # update the providers last synced date
    def _update_last_synced( self, provider: int ):

        debug_print_db(f"Updating last synced time for provider: {provider}")

        # with our database class
        with KP_DB( ) as db:

            db.call_proc( "Provider_Update_Refreshed", args=[provider], fetch=False )
            
        debug_print_db(f"Last synced time updated for provider: {provider}")

    # clean up the records in the database
    def _cleanup( self ):

        debug_print_db("Running database cleanup operations")

        # with our database class
        with KP_DB( ) as db:

            # execute the cleanup
            db.call_proc( "Streams_CleanUp", None, False )
            
        debug_print_db("Database cleanup completed")

    # fixup the records in the database
    def _fixup( self ):

        debug_print_db("Running database fixup operations")

        # with our database class
        with KP_DB( ) as db:

            # execute the cleanup
            db.call_proc( "Streams_FixUp", None, False )
            
        debug_print_db("Database fixup completed")
    
    # sync the streams
    def _sync_the_streams( self ):

        debug_print_db("Syncing streams from temp table to main table")

        # with our database class
        with KP_DB( ) as db:

            # call the sync sproc
            db.call_proc( "Streams_All_Sync", None, False )
            
        debug_print_db("Stream sync completed")

    # insert the streams into the temp table
    def _insert_the_streams( self, streams ):

        debug_print_db(f"Inserting {len(streams)} streams into temp table")

        # with our database class
        with KP_DB( ) as db:
        
            # insert the streams
            db.insert_many( 'stream_temp', streams, batch_size=2500 )
            
        debug_print_db(f"Successfully inserted {len(streams)} streams into temp table")
          
    # get the providers list
    def _get_providers( self, _provider: int = 0 ):

        debug_print_sync(f"Getting providers list (specific provider: {_provider})")

        # hold a return
        _ret = []

        # get the providers from the cache
        _ret = self.cache.get( self.cache_key_prov + str( _provider ) ) or None

        # if it's not in the cache yet
        if _ret is None:

            debug_print_sync("Providers not in cache, fetching from database")

            # with our database class
            with KP_DB( ) as db:

                # hold the where clause
                where = []

                # if we have a specific provider
                if _provider is not None and _provider != 0:

                    debug_print_sync(f"Filtering for specific provider ID: {_provider}")
                    # setup the where clause
                    where = [
                        WhereClause(
                            field="id", 
                            value=_provider,
                            operator=ComparisonOperator.EQ
                        )
                    ]
                    
                # get the provider records
                _ret = db.get_all( table='stream_providers', columns=['id', 
                        'u_id', 
                        'sp_should_filter', 
                        'sp_name', 
                        'sp_type', 
                        'sp_domain', 
                        'sp_username', 
                        'sp_password', 
                        'sp_stream_type',
                        'sp_refresh_period',
                        'sp_last_synced'], 
                        where=where )

            debug_print_sync(f"Retrieved {len(_ret)} providers from database")

            # set the item in the cache
            self.cache.set( self.cache_key_prov, _ret )
        else:
            debug_print_sync(f"Retrieved {len(_ret)} providers from cache")

        # return the providers
        return _ret

    # get the filters
    def _get_filters( self, uid: int ):

        debug_print_sync(f"Getting filters for user ID: {uid}")

        # hold a return
        _ret = []

        # hold and format the cache key
        _ckey = self.cache_key_filt % uid
        
        # get the providers from the cache
        _ret = self.cache.get( _ckey) or None

        # if we don't have the filters in the cache
        if _ret is None:

            debug_print_sync("Filters not in cache, fetching from database")

            # setup the where clause
            where = [
                WhereClause(
                    field="u_id", 
                    value=uid,
                    operator=ComparisonOperator.EQ
                ),
                WhereClause(
                    field="sf_active", 
                    value=1,
                    operator=ComparisonOperator.EQ
                ),
            ]

            # with our database class
            with KP_DB( ) as db:

                # get the filter records
                _ret = db.get_all( table='stream_filters',
                            columns=['id', 'sf_filter', 'sf_type_id'],
                            where=where )

            debug_print_sync(f"Retrieved {len(_ret)} filters from database for user {uid}")

            # clean up
            del where

            # set the item in the cache
            self.cache.set( _ckey, _ret )
        else:
            debug_print_sync(f"Retrieved {len(_ret)} filters from cache for user {uid}")

        # return the filters
        return _ret
    
    # get active streams for testing
    def _get_active_streams( self ):

        debug_print_sync("Getting active streams with provider info for testing")

        # with our database class
        with KP_DB( ) as db:

            # setup the where clause for active live and series streams
            where = [
                WhereClause(
                    field="s.s_active", 
                    value=1,
                    operator=ComparisonOperator.EQ
                ),
                WhereClause(
                    field="s.s_type_id", 
                    value=[0, 5],
                    operator=ComparisonOperator.IN,
                    connector="AND"
                ),
            ]

            # get the active stream records with provider connection limit info
            query = f"""
            SELECT s.id, s.s_orig_name, s.s_stream_uri, s.s_type_id, s.p_id,
                   p.sp_cnx_limit, p.sp_name
            FROM {db.table_prefix}streams s
            INNER JOIN {db.table_prefix}stream_providers p ON s.p_id = p.id
            WHERE s.s_active = 1 AND s.s_type_id IN (0, 5)
            """
            
            streams = db.execute_raw(query, fetch=True, dictionary=True)

        debug_print_sync(f"Retrieved {len(streams)} active streams with provider info from database")

        return streams

    # batch move invalid streams to other table
    def _batch_move_streams_to_other( self, stream_ids: list ):

        if not stream_ids:
            return

        debug_print_db(f"Batch moving {len(stream_ids)} streams to other table")

        # with our database class
        with KP_DB( ) as db:

            # Process in chunks to avoid overwhelming the database
            chunk_size = 100
            moved_count = 0
            
            for i in range(0, len(stream_ids), chunk_size):
                chunk = stream_ids[i:i + chunk_size]
                
                debug_print_db(f"Processing chunk {i//chunk_size + 1}: {len(chunk)} streams")
                
                # Use a single transaction for the chunk
                try:
                    with db.transaction():
                        for stream_id in chunk:
                            db.call_proc( "Streams_Move_To_Other", args=[stream_id], fetch=False )
                            moved_count += 1
                            
                    debug_print_db(f"Successfully moved chunk of {len(chunk)} streams")
                    
                except Exception as e:
                    debug_print_db(f"Error moving chunk starting at {i}: {e}")
                    # Continue with next chunk even if this one fails
                    continue
            
        debug_print_db(f"Batch move completed: {moved_count} streams moved to other table")
        return moved_count