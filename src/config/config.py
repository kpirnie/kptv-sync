#!/usr/bin/env python3
 
import json
from pathlib import Path
from typing import Any, Dict, Optional
import sys

# Global cache for config data
_config_cache: Optional[Dict[str, Any]] = None

def find_config(start_path: Path = Path(__file__)) -> Path:
    """Find the .kptvconf file in parent directories"""
    
    # Import debug utilities
    try:
        from utils.debug import debug_print_config_search
    except ImportError:
        # Fallback if debug utils not available
        def debug_print_config_search(msg): pass
    
    debug_print_config_search(f"Looking for .kptvconf file...")
    debug_print_config_search(f"__file__ = {__file__}")
    debug_print_config_search(f"sys.executable = {sys.executable}")
    debug_print_config_search(f"Current working directory = {Path.cwd()}")
    
    # Check if we're running as a compiled binary (PyInstaller)
    if getattr(sys, 'frozen', False):
        debug_print_config_search("Running as compiled binary")
        # We're running as a compiled binary
        # Look relative to the current working directory and executable location
        executable_path = Path(sys.executable)
        search_paths = [
            Path.cwd(),  # Current working directory
            Path.cwd() / 'src',  # src subdirectory of current working directory
            executable_path.parent,  # Same directory as executable
            executable_path.parent / 'src',  # src subdirectory relative to executable
            Path.home(),  # User home directory as fallback
        ]
        
        for search_dir in search_paths:
            config_path = search_dir / '.kptvconf'
            debug_print_config_search(f"Checking compiled binary location: {config_path}")
            if config_path.exists():
                debug_print_config_search(f"Found config at: {config_path}")
                return config_path
    
    else:
        debug_print_config_search("Running from source code")
        # We're running from source code
        # Use the original search method
        for parent in [start_path, *start_path.parents]:
            config_path = parent / '.kptvconf'
            debug_print_config_search(f"Checking source location: {config_path}")
            if config_path.exists():
                debug_print_config_search(f"Found config at: {config_path}")
                return config_path
    
    # If still not found, try some common locations
    debug_print_config_search("Trying common locations...")
    common_locations = [
        Path.cwd() / '.kptvconf',  # Current working directory
        Path.home() / '.kptvconf',  # Home directory
    ]
    
    for location in common_locations:
        debug_print_config_search(f"Checking common location: {location}")
        if location.exists():
            debug_print_config_search(f"Found config at: {location}")
            return location
    
    debug_print_config_search("No config file found in any location")
    # If not found, raise an error
    raise FileNotFoundError("Could not find .kptvconf in any parent directory or common locations")

def load_config() -> Dict[str, Any]:
    """Load configuration from file - loads fresh each time unless cached"""
    global _config_cache
    
    # If already cached, return it
    if _config_cache is not None:
        return _config_cache
    
    try:
        # Find the config file
        config_file = find_config()
        
        # Load and parse the config file
        raw_config = json.loads(config_file.read_text(encoding='utf-8'))
        
        # Validate required fields
        required_keys = {'dbserver', 'dbport', 'dbuser', 'dbpassword', 'dbschema', 'db_tblprefix'}
        if missing := required_keys - raw_config.keys():
            raise ValueError(f"Missing config keys: {missing}")
        
        # Cache the config
        _config_cache = raw_config
        return _config_cache
        
    except FileNotFoundError:
        raise RuntimeError("Config file .kptvconf not found in any parent directory")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in config: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load config: {e}")

def get_config() -> Dict[str, Any]:
    """Get the full config dictionary"""
    return load_config()

# Property functions that load config when called
def get_dbserver() -> str:
    return load_config()['dbserver']

def get_dbport() -> int:
    return load_config()['dbport']

def get_dbuser() -> str:
    return load_config()['dbuser']

def get_dbpassword() -> str:
    return load_config()['dbpassword']

def get_dbschema() -> str:
    return load_config()['dbschema']

def get_db_tblprefix() -> str:
    return load_config()['db_tblprefix']

# For backward compatibility - these will be loaded when first accessed
# Using module-level __getattr__ (Python 3.7+)
def __getattr__(name: str):
    """Load config values on demand when accessed as module attributes"""
    if name in ['DBSERVER', 'DBPORT', 'DBUSER', 'DBPASSWORD', 'DBSCHEMA', 'DB_TBLPREFIX']:
        config = load_config()
        
        # Map attribute names to config keys
        key_mapping = {
            'DBSERVER': 'dbserver',
            'DBPORT': 'dbport', 
            'DBUSER': 'dbuser',
            'DBPASSWORD': 'dbpassword',
            'DBSCHEMA': 'dbschema',
            'DB_TBLPREFIX': 'db_tblprefix'
        }
        
        if name in key_mapping:
            return config[key_mapping[name]]
    
    elif name == 'CONFIG_DICT':
        return load_config()
    
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")