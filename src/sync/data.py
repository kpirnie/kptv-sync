#!/usr/bin/env python3

# import the database class
from db.db import KP_DB, ComparisonOperator, WhereClause

class KP_Sync_Data:

    def __init__( self ):

        # setup the cache we're going to use
        from utils.cache import KP_Cache
        self.cache = KP_Cache( )

        # hold the cache keys
        self.cache_key_prov = "sync_providers"
        self.cache_key_filt = "sync_filters_%d"

    # update the providers last synced date
    def _update_last_synced( self, provider: int ):

        # with our database class
        with KP_DB( ) as db:

            db.call_proc( "Provider_Update_Refreshed", args=[provider], fetch=False )

    # clean up the records in the database
    def _cleanup( self ):

        # with our database class
        with KP_DB( ) as db:

            # execute the cleanup
            db.call_proc( "Streams_CleanUp", None, False )

    # fixup the records in the database
    def _fixup( self ):

        # with our database class
        with KP_DB( ) as db:

            # execute the cleanup
            db.call_proc( "Streams_FixUp", None, False )
    
    # sync the streams
    def _sync_the_streams( self ):

        # with our database class
        with KP_DB( ) as db:

            # call the sync sproc
            db.call_proc( "Streams_All_Sync", None, False )

    # insert the streams into the temp table
    def _insert_the_streams( self, streams ):

        # with our database class
        with KP_DB( ) as db:
        
            # insert the streams
            db.insert_many( 'stream_temp', streams, batch_size=2500 )
          
    # get the providers list
    def _get_providers( self, _provider: int = 0 ):

        # hold a return
        _ret = []

        # get the providers from the cache
        _ret = self.cache.get( self.cache_key_prov + str( _provider ) ) or None

        # if it's not in the cache yet
        if _ret is None:

            # with our database class
            with KP_DB( ) as db:

                # hold the where clause
                where = []

                # if we have a specific provider
                if _provider is not None and _provider != 0:

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

            # set the item in the cache
            self.cache.set( self.cache_key_prov, _ret )

        # return the providers
        return _ret

    # get the filters
    def _get_filters( self, uid: int ):

        # hold a return
        _ret = []

        # hold and format the cache key
        _ckey = self.cache_key_filt % uid
        
        # get the providers from the cache
        _ret = self.cache.get( _ckey) or None

        # if we don't have the filters in the cache
        if _ret is None:

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

            # clean up
            del where

            # set the item in the cache
            self.cache.set( _ckey, _ret )

        # return the filters
        return _ret
