#!/usr/bin/env python3

"""
Debug utilities to control debug output throughout the application
"""

import sys
from typing import Any, Optional

# Global debug state
_debug_enabled: Optional[bool] = None

def set_debug(enabled: bool) -> None:
    """Set the global debug state"""
    global _debug_enabled
    _debug_enabled = enabled

def is_debug_enabled() -> bool:
    """Check if debug mode is enabled"""
    global _debug_enabled
    
    # If not explicitly set, try to detect from command line args
    if _debug_enabled is None:
        # Try to detect --debug flag from sys.argv
        _debug_enabled = '--debug' in sys.argv
    
    return _debug_enabled

def debug_print(*args, **kwargs) -> None:
    """Print debug message only if debug mode is enabled"""
    if is_debug_enabled():
        print("DEBUG:", *args, **kwargs)

def debug_print_config_search(message: str) -> None:
    """Print config search debug message"""
    if is_debug_enabled():
        print(f"DEBUG CONFIG: {message}")

def debug_print_db(message: str) -> None:
    """Print database debug message"""
    if is_debug_enabled():
        print(f"DEBUG DB: {message}")

def debug_print_sync(message: str) -> None:
    """Print sync debug message"""
    if is_debug_enabled():
        print(f"DEBUG SYNC: {message}")

def debug_print_request(message: str) -> None:
    """Print request debug message"""
    if is_debug_enabled():
        print(f"DEBUG REQUEST: {message}")

# Context manager for conditional debug output
class DebugContext:
    """Context manager that only executes if debug is enabled"""
    
    def __init__(self):
        self.enabled = is_debug_enabled()
    
    def __enter__(self):
        return self.enabled
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

# Usage examples:
# 
# from utils.debug import debug_print, DebugContext, set_debug
# 
# # In main or common initialization:
# set_debug(args.debug)
# 
# # Throughout the code:
# debug_print("This only prints in debug mode")
# 
# with DebugContext() as debug:
#     if debug:
#         # This block only executes in debug mode
#         expensive_debug_operation()