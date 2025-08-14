#!/usr/bin/env python3

# Common imports
import os
from typing import Iterator, Optional, Union, Dict, List, Any, Tuple, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum

# PyMySQL imports
try:
    import pymysql
    from pymysql import Error
except ImportError:
    raise ImportError("PyMySQL is required. Please install it with: pip install PyMySQL")

# Set locale - handle systems that don't have en_US.UTF-8
try:
    os.environ['LANG'] = 'en_US.UTF-8'
except:
    try:
        os.environ['LANG'] = 'C.UTF-8'
    except:
        # Fallback to C locale
        os.environ['LANG'] = 'C'

# Import debug utilities
try:
    from utils.debug import debug_print_db
except ImportError:
    def debug_print_db(msg): pass

debug_print_db("Using PyMySQL for database connections")

# Define enums for join types
class JoinType( Enum ):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"

# Define enums for comparison operators
class ComparisonOperator( Enum ):
    EQ = "="
    NE = "!="
    LT = "<"
    GT = ">"
    LTE = "<="
    GTE = ">="
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    IN = "IN"
    NOT_IN = "NOT IN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    BETWEEN = "BETWEEN"
    REGEXP = "REGEXP"
    NOT_REGEXP = "NOT REGEXP"

# Define data classes for SQL JOIN clauses
@dataclass
class JoinClause:
    table: str
    left_field: str
    right_field: str
    operator: ComparisonOperator = ComparisonOperator.EQ
    join_type: JoinType = JoinType.INNER

    # Define the join condition based on the operator
    def __str__( self ):
        return f"{self.join_type.value} JOIN {self.table} ON {self.left_field} {self.operator.value} {self.right_field}"

# Define data classes for SQL WHERE clauses
@dataclass
class WhereClause:
    field: str
    value: Any
    operator: ComparisonOperator = ComparisonOperator.EQ
    connector: str = "AND"  # AND or OR for combining with other clauses

    # Define the where condition based on the operator
    def __str__( self ):

        # if we're comparing nulls
        if self.operator in [ComparisonOperator.IS_NULL, ComparisonOperator.IS_NOT_NULL]:
            return f"{self.field} {self.operator.value}"
        
        # otherwise if we're comparing between
        elif self.operator == ComparisonOperator.BETWEEN:
            return f"{self.field} {self.operator.value} %s AND %s"
        
        # otherwise if we're comparing IN
        elif self.operator in [ComparisonOperator.IN, ComparisonOperator.NOT_IN]:
            if isinstance(self.value, (list, tuple)):
                placeholders = ', '.join(['%s'] * len(self.value))
                return f"{self.field} {self.operator.value} ({placeholders})"
            raise ValueError("IN/NOT IN operator requires a list/tuple of values")
        else:

            # for all other operators, we just return the field and operator
            return f"{self.field} {self.operator.value} %s"

# Define data classes for SQL ORDER BY clauses
@dataclass
class OrderByClause:
    column: str
    direction: str = "ASC"

# Simple connection pool for PyMySQL
class PyMySQLConnectionPool:
    def __init__(self, **kwargs):
        self.connection_params = kwargs
        self.pool_name = kwargs.get('pool_name', 'default')
        
    def get_connection(self):
        return pymysql.connect(
            host=self.connection_params['host'],
            port=self.connection_params['port'],
            user=self.connection_params['user'],
            password=self.connection_params['password'],
            database=self.connection_params['database'],
            charset='utf8mb4',
            autocommit=False
        )

# Main database class
class KP_DB:

    # initialize the database class
    def __init__( self, pool_size: int = 4, chunk_size: int = 1000 ):

        # import our common module
        from config.config import DBSERVER, DBPORT, DBUSER, DBPASSWORD, DBSCHEMA, DB_TBLPREFIX

        debug_print_db(f"Initializing PyMySQL database connection")
        debug_print_db(f"Server: {DBSERVER}:{DBPORT}, Database: {DBSCHEMA}, User: {DBUSER}")

        # set the class variables
        self.host = DBSERVER
        self.port = DBPORT
        self.database = DBSCHEMA
        self.user = DBUSER
        self.password = DBPASSWORD
        self.table_prefix = DB_TBLPREFIX
        self.pool_size = pool_size
        self.chunk_size = chunk_size
        self.connection_pool = self._initialize_pool( )

        debug_print_db(f"PyMySQL database connection initialized successfully")

    # destructor to close the connection pool when we're done
    def __del__( self ):

        # if we have a connection pool
        if hasattr( self, 'connection_pool' ) and self.connection_pool is not None:
            debug_print_db("Closing database connection pool")
            # nullify the connection pool
            del self.connection_pool
            self.connection_pool = None

    # context manager to handle the connection pool
    def __enter__( self ):
        return self

    # context manager to handle the automagic closing of the connection pool
    def __exit__( self, exc_type, exc_val, exc_tb ):

        # if we have a connection pool
        if hasattr( self, 'connection_pool' ) and self.connection_pool is not None:
            debug_print_db("Closing database connection pool (context manager)")
            # nullify the connection pool
            del self.connection_pool
            self.connection_pool = None

    # initialize the connection pool
    def _initialize_pool( self ):

        # create the connection pool
        try:
            debug_print_db(f"Creating PyMySQL connection pool")

            pool = PyMySQLConnectionPool(
                pool_name="kptv_db_pool",
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            
            debug_print_db("PyMySQL connection pool created successfully")
            return pool
        
        # if we run into an error, raise a connection error
        except Exception as e:
            debug_print_db(f"Failed to create connection pool: {e}")
            raise ConnectionError( f"Failed to create connection pool: {e}" )

    # get a connection from the pool
    def _get_connection( self ):

        # try to get a connection from the pool
        try:

            # hold it
            conn = self.connection_pool.get_connection( )

            debug_print_db("Successfully obtained PyMySQL connection from pool")
            return conn
                
        # if there was an error
        except Exception as e:
            debug_print_db(f"Error getting connection from pool: {e}")
            raise ConnectionError( f"Error getting connection from pool: {e}" )

    # context manager to handle the cursor
    @contextmanager
    def _get_cursor( self, dictionary: bool = True, buffered: bool = False ):

        # setup the connection and cursor
        conn = None
        cursor = None

        # try to get a connection from the pool
        try:
            debug_print_db("Getting connection from pool")

            # get a connection from the pool
            conn = self._get_connection( )

            # get a cursor from the connection
            if dictionary:
                cursor = conn.cursor(pymysql.cursors.DictCursor)
            else:
                cursor = conn.cursor()

            # yield the cursor
            yield cursor

            # commit the changes
            conn.commit( )
            debug_print_db("Transaction committed successfully")

        # if we run into an error, rollback the changes
        except Exception as e:
            debug_print_db(f"Database error occurred, rolling back: {e}")
            if conn:
                conn.rollback( )
            raise RuntimeError( f"Database error: {e}" )
        
        # and finally, close the cursor and connection
        finally:
            if cursor:
                cursor.close( )
            if conn:
                conn.close( )
            debug_print_db("Cursor and connection closed")

    # execute a query with the cursor
    def _execute( self, query: str, params=None, fetch: bool = True, dictionary: bool = True, stream: bool = False ) -> Any:

        debug_print_db(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
        if params:
            debug_print_db(f"Query parameters: {params}")

        # if we're streaming results, we need to set the buffered flag to False
        if stream:
            dictionary = False

        # with the cursor, execute the query and return the results
        with self._get_cursor( dictionary, not stream ) as cursor:

            # execute the query with the provided parameters
            cursor.execute( query, params or ( ) )

            # if we're not fetching results, return None
            if not fetch:
                debug_print_db(f"Query executed, {cursor.rowcount} rows affected")
                return None
            
            # if we're streaming results, return an iterator
            if stream:
                debug_print_db("Returning streaming results")
                return self._stream_results( cursor )
            
            # otherwise, fetch the results and return them
            results = cursor.fetchall( )
            debug_print_db(f"Query returned {len(results) if results else 0} rows")
            return results

    # stream results from the cursor
    def _stream_results( self, cursor ) -> Iterator[Dict]:

        # while there are still rows to fetch, yield them
        while True:

            # fetch the next chunk of rows
            rows = cursor.fetchmany( self.chunk_size )

            # if there are no more rows, break the loop
            if not rows:
                break

            debug_print_db(f"Streaming {len(rows)} rows")
            # yield the rows
            yield from rows

    # Unified WHERE clause builder
    def _build_where_clause( self, where: List[WhereClause] ) -> Tuple[str, List[Any]]:

        # if there are no where clauses, return an empty string and an empty list
        if not where:
            return "", []
        
        debug_print_db(f"Building WHERE clause with {len(where)} conditions")
        
        # setup the where clause and parameters
        where_parts = []
        where_params = []
        
        # loop through the where clauses and build the where clause string
        for i, clause in enumerate( where ):

            # if the clause is a string, use it as is
            where_str = str( clause )
            
            # if we're working with BETWEEN
            if clause.operator == ComparisonOperator.BETWEEN:
                if isinstance( clause.value, ( list, tuple ) ) and len( clause.value ) == 2:
                    where_params.extend( clause.value )
                else:
                    raise ValueError( "BETWEEN operator requires a list/tuple with exactly 2 values" )
                
            # if were working with IN or NOT IN
            elif clause.operator in [ComparisonOperator.IN, ComparisonOperator.NOT_IN]:
                if isinstance( clause.value, ( list, tuple ) ):
                    where_params.extend( clause.value )
                else:
                    raise ValueError( "IN/NOT IN operator requires a list/tuple of values" )
                
            # if we're workgin with NULLS
            elif clause.operator not in [ComparisonOperator.IS_NULL, ComparisonOperator.IS_NOT_NULL]:
                where_params.append( clause.value )
            
            # if we're working with REGEXP or NOT REGEXP
            elif clause.operator in [ComparisonOperator.REGEXP, ComparisonOperator.NOT_REGEXP]:
                if not isinstance( clause.value, str ):
                    raise ValueError( "REGEXP/NOT REGEXP operator requires a string value" )
                where_params.append( clause.value )

            # if this is the first clause, just add it to the list
            if i == 0:
                where_parts.append( where_str )
            else:
                where_parts.append( f"{clause.connector} {where_str}" )
        
        # join the where parts and return the where clause and parameters
        where_clause = " WHERE " + " ".join( where_parts )
        debug_print_db(f"Built WHERE clause: {where_clause}")
        return where_clause, where_params

    # build the SELECT query with all options
    def _build_select_query( self, table: str, columns: List[str] = None, 
                          joins: List[JoinClause] = None,
                          where: List[WhereClause] = None, 
                          group_by: str = None,
                          having: str = None, 
                          order_by: List[OrderByClause] = None,
                          limit: int = None, 
                          offset: int = None ) -> Tuple[str, List[Any]]:
        
        debug_print_db(f"Building SELECT query for table: {table}")
        
        # setup the columns to select
        cols = "*" if not columns else ", ".join( columns )

        # setup the table name
        full_table = f"{self.table_prefix}{table}" if self.table_prefix else table
        
        # setup the SQL query string
        query = f"SELECT {cols} FROM {full_table}"
        
        # hold the params
        params = []

        # if there are any joins
        if joins:
            debug_print_db(f"Adding {len(joins)} JOIN clauses")
            # loop them and add them to the query
            for join in joins:
                query += f" {join}"

        # if there is a WHERE clause
        if where:

            # set it up
            where_clause, where_params = self._build_where_clause( where )
            
            # now add it to the query
            query += where_clause

            # and setup it's parameters
            params.extend( where_params )

        # if we need to group by
        if group_by:
            debug_print_db(f"Adding GROUP BY: {group_by}")
            # append to the query string
            query += f" GROUP BY {group_by}"

        # if we need HAVING
        if having:
            debug_print_db(f"Adding HAVING: {having}")
            # append to the query string
            query += f" HAVING {having}"

        # if we need to order the query
        if order_by:
            debug_print_db(f"Adding ORDER BY with {len(order_by)} clauses")
            # hold the clauses
            order_clauses = []

            # loop over the list
            for ob in order_by:

                # hold the direction
                direction = "DESC" if ob.direction.upper( ) == "DESC" else "ASC"

                # combine them all
                order_clauses.append( f"{ob.column} {direction}" )

            # now add them to the query
            query += f" ORDER BY {', '.join( order_clauses )}"

        # if we're limitting the return
        if limit is not None:
            debug_print_db(f"Adding LIMIT: {limit}")
            # append it to the string
            query += f" LIMIT {limit}"

            # along with the offset if it exists
            if offset is not None:
                debug_print_db(f"Adding OFFSET: {offset}")
                query += f" OFFSET {offset}"

        debug_print_db(f"Built query: {query}")
        # return the query and parameters
        return query, params

    # Transaction Support
    @contextmanager
    def transaction( self ):
        
        debug_print_db("Starting database transaction")
        
        # setup the connection
        conn = None

        # try to get a connection
        try:

            # the connection
            conn = self._get_connection()

            # yield the connection pool
            yield conn

            # commit the transaction
            conn.commit( )
            debug_print_db("Transaction committed successfully")

        # if there's an error
        except Exception as e:
            debug_print_db(f"Transaction failed, rolling back: {e}")

            # roll back the transaction
            if conn:
                conn.rollback( )
            raise

        # and finally, close the connection
        finally:
            if conn:
                conn.close( )
                debug_print_db("Transaction connection closed")

    # get a single record for the query
    def get_one( self, 
                table: str, 
                columns: List[str] = None, 
                joins: List[JoinClause] = None,
                where: List[WhereClause] = None,
                group_by: str = None, having: str = None,
                order_by: List[OrderByClause] = None ) -> Optional[Dict]:
        
        debug_print_db(f"Getting single record from table: {table}")
        
        # setup the query and parameters
        query, params = self._build_select_query(
            table=table,
            columns=columns,
            joins=joins,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            limit=1
        )

        # execute the query
        result = self._execute( query, params=params, fetch=True, dictionary=True )

        # return a result
        return result[0] if result else None

    # get all records
    def get_all( self, 
                table: str, 
                columns: List[str] = None, 
                joins: List[JoinClause] = None,
                where: List[WhereClause] = None,
                group_by: str = None, 
                having: str = None,
                order_by: List[OrderByClause] = None,
                limit: int = None, offset: int = None,
                stream: bool = False ) -> Union[List[Dict], Iterator[Dict]]:
        
        debug_print_db(f"Getting all records from table: {table}")
        
        # setup the query and the parameters
        query, params = self._build_select_query(
            table=table,
            columns=columns,
            joins=joins,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            limit=limit,
            offset=offset
        )

        # return the records
        return self._execute( query, params=params, fetch=True, dictionary=True, stream=stream )

    # get chunked results
    def get_chunked( self, table: str, columns: List[str] = None, 
                   joins: List[JoinClause] = None,
                   where: List[WhereClause] = None,
                   group_by: str = None, having: str = None,
                   order_by: List[OrderByClause] = None ) -> Iterator[List[Dict]]:
        
        debug_print_db(f"Getting chunked results from table: {table}")
        
        # hold the offset
        offset = 0

        # while we have valid results
        while True:

            # setup the query and parameters
            query, params = self._build_select_query(
                table=table,
                columns=columns,
                joins=joins,
                where=where,
                group_by=group_by,
                having=having,
                order_by=order_by,
                limit=self.chunk_size,
                offset=offset
            )

            # setup the results
            results = self._execute( query, params=params )

            # if there are none
            if not results:

                # break the loop
                break

            debug_print_db(f"Yielding chunk with {len(results)} records (offset: {offset})")
            # yield the results
            yield results

            # setup the next offset
            offset += len( results )

    # execut an insert
    def insert( self, table: str, data: Dict, return_id: bool = True ) -> Optional[int]:

        # if there's no data we cant do anything
        if not data:
            raise ValueError( "No data provided for insert" )
            
        debug_print_db(f"Inserting single record into table: {table}")
        
        # setup the columns
        columns = ", ".join( data.keys( ) )

        # setup the placeholders
        placeholders = ", ".join( ["%s"] * len( data ) )

        # setup the table
        full_table = f"{self.table_prefix}{table}" if self.table_prefix else table
        
        # now build the full query
        query = f"INSERT INTO {full_table} ({columns}) VALUES ({placeholders})"
        
        # with a cursor
        with self._get_cursor( ) as cursor:

            # execute the insert query
            cursor.execute( query, tuple( data.values( ) ) )

            # retur the last inserted id, or none
            inserted_id = cursor.lastrowid if return_id else None
            debug_print_db(f"Insert completed, ID: {inserted_id}")
            return inserted_id

    # insert many records
    def insert_many( self, table: str, data: List[Dict], return_ids: bool = False, ignore_duplicates: bool = True, batch_size: int = 1000 ) -> Optional[List[int]]:
        
        # if there's no data
        if not data:
            raise ValueError( "No data provided for insert" )

        debug_print_db(f"Inserting {len(data)} records into table: {table} (batch_size: {batch_size})")

        # setup the columns
        columns = ", ".join( data[0].keys( ) )

        # setup the placeholders
        placeholders = ", ".join( ["%s"] * len( data[0] ) )

        # setup the full table name
        full_table = f"{self.table_prefix}{table}" if self.table_prefix else table

        # setup the rest of the base query
        ignore_keyword = "IGNORE" if ignore_duplicates else ""
        base_query = f"INSERT {ignore_keyword} INTO {full_table} ({columns}) VALUES ({placeholders})"

        # try to run the query
        try:

            # Utilize the cursor
            with self._get_cursor( ) as cursor:

                # if we want to return the ids
                if return_ids:
                    debug_print_db("Performing individual inserts to track IDs")

                    # Individual inserts for accurate ID tracking
                    inserted_ids = []

                    # for each row in the insertable data provided
                    for row in data:

                        # see if we can trap an error
                        try:

                            # execute the query
                            cursor.execute( base_query, tuple( row.values( ) ) )
                            
                            # grab the last inserted ids
                            inserted_ids.append( cursor.lastrowid )

                        # try to trap errors
                        except Exception as e:

                            # ignore duplicates
                            if ignore_duplicates and ("Duplicate entry" in str(e) or "1062" in str(e)):
                                debug_print_db(f"Ignoring duplicate key error for row")
                                continue
                            raise

                    debug_print_db(f"Individual inserts completed, {len(inserted_ids)} IDs returned")
                    # return the captured ids
                    return inserted_ids

                # otherwise
                else:
                    debug_print_db("Performing batch inserts for better performance")

                    # Batch processing for better performance
                    values = [tuple( row.values( ) ) for row in data]
                    
                    # Split into batches to avoid huge queries
                    for i in range( 0, len( values ), batch_size ):

                        # setup a batch to run
                        batch = values[i:i + batch_size]
                        debug_print_db(f"Processing batch {i//batch_size + 1}: {len(batch)} records")

                        # try to execute
                        try:

                            # execute the query
                            cursor.executemany( base_query, batch )

                        # trap errors
                        except Exception as e:

                            # if we're configured to ignore duplicate records
                            if ignore_duplicates and ("Duplicate entry" in str(e) or "1062" in str(e)):
                                debug_print_db("Batch failed with duplicates, falling back to individual inserts")

                                # Fall back to individual inserts for the failed batch
                                for value in batch:
                                    try:
                                        cursor.execute( base_query, value )
                                    except Exception as e:
                                        if ignore_duplicates and ("Duplicate entry" in str(e) or "1062" in str(e)):
                                            continue
                                        raise
                             # otherwise raise an error
                            else:
                                raise
                    
                    debug_print_db("Batch inserts completed successfully")
                    # return nothing
                    return None
        # trap errors
        except Exception as e:

            # check if we're ignoring duplicates
            if ignore_duplicates and ("Duplicate entry" in str(e) or "1062" in str(e)):
                debug_print_db("Insert failed with duplicate key error (ignored)")
                return [] if return_ids else None
            
            # otherwise... 
            debug_print_db(f"Insert failed with error: {e}")
            raise RuntimeError( f"Database error during insert: {e}" ) from e

    # update a record
    def update( self, table: str, where: List[WhereClause], data: Dict ) -> int:

        # if there's no data, we can't do anything
        if not data:
            raise ValueError( "No data provided for update" )
        
        debug_print_db(f"Updating records in table: {table}")
        
        # setup the set clause
        set_clause = ", ".join( [f"{key} = %s" for key in data.keys( )] )

        # setup the table
        full_table = f"{self.table_prefix}{table}" if self.table_prefix else table

        # setup the where clause
        where_clause, where_params = self._build_where_clause( where )

        # setup the query
        query = f"UPDATE {full_table} SET {set_clause}{where_clause}"

        # setup the parameters
        params = list( data.values( ) ) + where_params
        
        # with a cursor
        with self._get_cursor( ) as cursor:

            # execute the update query
            cursor.execute( query, params )

            # return the number of rows affected
            rows_affected = cursor.rowcount
            debug_print_db(f"Update completed, {rows_affected} rows affected")
            return rows_affected

    # delete a record(s)
    def delete( self, table: str, where: List[WhereClause] ) -> int:

        debug_print_db(f"Deleting records from table: {table}")
        
        # setup the table
        full_table = f"{self.table_prefix}{table}" if self.table_prefix else table

        # setup the where clause
        where_clause, where_params = self._build_where_clause( where )

        # setup the query
        query = f"DELETE FROM {full_table}{where_clause}"
        
        # with the cursor
        with self._get_cursor( ) as cursor:

            # execute the delete query
            cursor.execute( query, where_params )

            # return the number of rows affected
            rows_affected = cursor.rowcount
            debug_print_db(f"Delete completed, {rows_affected} rows affected")
            return rows_affected

    # call a stored procedure
    def call_proc(self, procedure_name: str, args=None, fetch: bool = False):
        
        debug_print_db(f"Calling stored procedure: {procedure_name}")
        if args:
            debug_print_db(f"Procedure arguments: {args}")
        
        # with our cursor
        with self._get_cursor( dictionary=True ) as cursor:

            # try to call the procedure
            try:
                # Call the procedure
                cursor.callproc( procedure_name, args or ( ) )
                
                # if we're not supposed to fetch anything
                if not fetch:
                    rows_affected = cursor.rowcount
                    debug_print_db(f"Procedure executed, {rows_affected} rows affected")
                    return rows_affected
                
                # For procedures that return results
                results = cursor.fetchall()
                
                debug_print_db(f"Procedure returned {len(results) if results else 0} rows")
                    
                # Return results
                return results if results else None
                
            # trapped an error
            except Exception as e:
                debug_print_db(f"Procedure call failed: {e}")
                # Handle cases where there are no results to fetch
                if not fetch:
                    debug_print_db("Procedure completed (no results to fetch)")
                    return cursor.rowcount if hasattr(cursor, 'rowcount') else 0
                return None
    
    # execute a raw query
    def execute_raw( self, query: str, params=None, fetch: bool = False, dictionary: bool = True ):
        
        debug_print_db(f"Executing raw query: {query[:100]}{'...' if len(query) > 100 else ''}")
        
        # with the cursor
        with self._get_cursor( dictionary=dictionary ) as cursor:
            
            # execute the query
            cursor.execute( query, params or ( ) )
            
            # if we're not expected to return anything
            if not fetch:
                rows_affected = cursor.rowcount
                debug_print_db(f"Raw query executed, {rows_affected} rows affected")
                return rows_affected
            
            # otherwise we can try
            try:

                # setup the retults to be returned
                results = cursor.fetchall( )
                
                debug_print_db(f"Raw query returned {len(results) if results else 0} rows")
                # return them
                return results if results else None
            
            # trapped an error
            except Exception:
                debug_print_db("Raw query completed (no results available)")
                return None