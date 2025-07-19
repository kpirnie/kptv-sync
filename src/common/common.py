#!/usr/bin/env python3

# common imports
import subprocess, sys, argparse
from argparse import RawTextHelpFormatter, SUPPRESS

# our common class
class KP_Common:

    # constructor
    def __init__( self ):

        # get the python version.    This will only work with v3.9+
        _py_ver = sys.version_info
        if _py_ver[0] < 3 and _py_ver[1] < 10:
            print( "*" * 76 )
            print ( "You must be running at least Python 3.10" )
            print( "You are currently running version: {}.{}.{}\n".format( _py_ver[0], _py_ver[1], _py_ver[2] ) )
            print( "*" * 76 )
            sys.exit( )

        # handle the arguments
        self.actions = None
        self.args = None
        self.__handle_args( )

        # hold our common variables
        self.api_live = "%s/player_api.php?username=%s&password=%s&action=get_live_streams"
        self.api_series = "%s/player_api.php?username=%s&password=%s&action=get_series"
        self.api_vod = "%s/player_api.php?username=%s&password=%s&action=get_vod_streams"
        self.stream_live = "%s/live/%s/%s/%s.%s"
        self.stream_series = "%s/series/%s/%s/%s.%s"
        self.stream_vod = "%s/movie/%s/%s/%s.%s"

    # handle the arguments
    def __handle_args( self ):

        # fire up the argument parser
        _args = argparse.ArgumentParser( allow_abbrev=False, description=SUPPRESS, usage=SUPPRESS, formatter_class=RawTextHelpFormatter, add_help=False )

        # Add a own help option
        _args.add_argument('-h', dest='help', action='store_true', help=SUPPRESS)

        # we need an action argument at the very least
        _args.add_argument( "-a", required=False, dest='action', help=SUPPRESS )

        # add in the rest of the arguments
        _args.add_argument( "--live", action="store_true", help=SUPPRESS )
        _args.add_argument( "--series", action="store_true", help=SUPPRESS )
        _args.add_argument( "--vod", action="store_true", help=SUPPRESS )
        _args.add_argument( "--provider", type=int, help=SUPPRESS )
        _args.add_argument( "--debug", action="store_true", help=SUPPRESS )

        # Safe init
        _the_args = None
        unknown = []

        # Try to parse arguments safely
        try:
            _the_args, unknown = _args.parse_known_args( )
        except SystemExit:
            print( "*" * 76 )
            self.kp_print( "error", "You must pass at least 1 argument." )
            self.custom_help( )
            sys.exit() # Exit here after showing help in exception

        # Handle help manually
        if _the_args is not None and _the_args.help:
            self.custom_help( )
            sys.exit( ) # Exit after displaying help

        # hold our action
        _action = _the_args.action
        # set the action to lower case
        _action = self.arg_to_lower( _action )

        # check for the action
        if _action is None:
            print( "*" * 76 )
            self.kp_print( "error", "You must pass at least 1 argument." )
            self.custom_help( )
            sys.exit( )

        # Validate action manually
        if _action not in ['sync', 'fixup']:
            print("*" * 76)
            self.kp_print("error", f"'{_action}' is not a valid action.")
            self.custom_help( )
            sys.exit()

        # set the action and arguments
        self.actions = _action.lower()
        self.args = _the_args

    # our custom help message
    def custom_help( self ):
        print( "*" * 76 )
        print( '''usage: \033[92m./main.py [-h] -a {sync,fixup} [options]\033[37m
\t\033[94msync\033[37m: Sync the streams from the providers to the stream manager.
\t\t\033[93mOPTIONS:\033[37m
\t\t\t\033[94m--live\033[37m Sync all live streams.
\t\t\t\033[94m--series\033[37m Sync all series streams.
\t\t\t\033[94m--vod\033[37m Sync all vod streams.
\t\t\t\033[94m--provider [###]\033[37m Sync only the streams for the specified provider id.              
\t\033[94mfixup\033[37m: Fix all streams.
\t\tThis attempts to match channel numbers, logos, and tvg-id's for all streams.
''' )
        print( "*" * 76 )

    # our pretty printer ;)
    def kp_print( self, _type, _str ):

        # default to white, or reset
        _reset = "\033[37m"

        # if it's an error
        if _type.lower( ) == "error":

            # red
            _prefix = "\033[91m"
        
        # if it's a success
        if _type.lower( ) == "success":

            # green
            _prefix = "\033[92m"

        # if it's a warning
        if _type.lower( ) == "warn":

            # yellow
            _prefix = "\033[93m"

        # if it's a informative message
        if _type.lower( ) == "info":

            # blue
            _prefix = "\033[94m"

        # print the output based on the selected message type
        print( "{}{}{}".format( _prefix, _str, _reset ) )

    # print a line of stars
    def kp_print_line( self ):

        self.kp_print( "success", "*" * 76 )

    # argument to lower
    def arg_to_lower( self, _arg ):

        if _arg is None:

            # return an empty string
            return ""
        else:

            # return the lowercase string
            return _arg.lower( )
