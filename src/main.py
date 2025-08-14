#!/usr/bin/env python3

# import common imports
import sys, time

# Import debug utilities
try:
    from utils.debug import debug_print, set_debug
except ImportError:
    def debug_print(msg): pass
    def set_debug(enabled): pass

from common.common import KP_Common
from db.db import KP_DB
# import the sync class
from sync.sync import KP_Sync

# fire up the common class
common = KP_Common( )

# Initialize debug mode based on args
if hasattr(common.args, 'debug'):
    set_debug(common.args.debug)

debug_print("Starting application")
debug_print(f"Action: {common.actions}")

sync = KP_Sync( )

# wrap all the actions in a try block
try:

    # use the new match statement to handle the actions
    match common.actions:

        # sync the streams
        case "sync":

            debug_print("Starting sync operation")
            
            # NO PRINT STATEMENTS HERE - only the sync summary should show
            
            # run the sync
            sync.sync( )
            del sync

            debug_print("Sync operation completed")
            sys.exit( )

        # fixup the streams
        case "fixup":

            debug_print("Starting fixup operation")
            
            # NO PRINT STATEMENTS HERE - only essential output should show
            
            # the fixup method
            sync.fixup( )
            del sync

            debug_print("Fixup operation completed")
            sys.exit( )

        # if we don't have a match, show the help
        case _:

            # This should still show - it's user-facing help
            common.kp_print_line( )
            common.kp_print( "error", "You must pass at least 1 argument." )
            common.custom_help( )
            sys.exit( )

# catch the keyboard interrupt
except KeyboardInterrupt:

    # show a message then exit - this should still be visible
    print()
    common.kp_print_line( )
    common.kp_print( "info", "Exitting the app, please hold." )
    common.kp_print_line( )
    time.sleep( 5 )
    sys.exit( )
    
except Exception as e:
    # Error messages should still be visible
    debug_print(f"Unexpected error: {e}")
    common.kp_print( "error", f"An unexpected error occurred: {str(e)}" )
    sys.exit( 1 )