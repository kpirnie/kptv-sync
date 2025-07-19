#!/usr/bin/env python3

# import common imports
import sys, time
from common.common import KP_Common
from db.db import KP_DB
# import the sync class
from sync.sync import KP_Sync
sync = KP_Sync( )

# fire up the common class
common = KP_Common( )

# wrap all the actions in a try block
try:

    # use the new match statement to handle the actions
    match common.actions:

        # sync the streams
        case "sync":

            print
            common.kp_print_line( )
            common.kp_print( "info", "Please hold while we sync the provider data" )

            # run the sync
            sync.sync( )
            del sync

            # all set
            common.kp_print( "info", "All done" )
            common.kp_print_line( )
            sys.exit( )

        # fixup the streams
        case "fixup":

            print
            common.kp_print_line( )
            common.kp_print( "info", "Please hold while we fix up the data" )
            
            # the fixup method
            sync.fixup( )
            del sync

            common.kp_print( "info", "All done" )
            common.kp_print_line( )
            sys.exit( )

        # if we don't have a match, show the help
        case _:

            common.kp_print_line( )
            common.kp_print( "error", "You must pass at least 1 argument." )
            common.custom_help( )
            sys.exit( )

# catch the keyboard interrupt
except KeyboardInterrupt:

    # show a message then exit
    print
    common.kp_print_line( )
    common.kp_print( "info", "Exitting the app, please hold." )
    common.kp_print_line( )
    time.sleep( 5 )
    sys.exit( )
    