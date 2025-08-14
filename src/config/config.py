import json
from pathlib import Path
from typing import Any, Dict
import sys

# find_config function to search for .kptvconf file in parent directories
def find_config( start_path: Path = Path( __file__ ) ) -> Path:
    
    print(f"DEBUG: Starting config search from: {start_path}")
    print(f"DEBUG: Absolute starting path: {start_path.resolve()}")
    
    # find the path to the config file
    for parent in [start_path, *start_path.parents]:
        config_path = parent / '.kptvconf'
        print(f"DEBUG: Checking for config at: {config_path.resolve()}")
        
        if config_path.exists():
            print(f"DEBUG: Found config file at: {config_path.resolve()}")
            return config_path
        
    # If not found, raise an error
    error_msg = f"Could not find .kptvconf in any parent directory starting from {start_path.resolve()}"
    print(f"DEBUG: {error_msg}")
    raise FileNotFoundError(error_msg)

# hold the config file
try:
    _CONFIG_FILE = find_config()
    print(f"DEBUG: Using config file: {_CONFIG_FILE.resolve()}")
except Exception as e:
    print(f"ERROR: Config file search failed: {e}")
    sys.exit(1)

# Check if the config file exists and is readable
try:
    
    # Load and parse the config file
    config_content = _CONFIG_FILE.read_text(encoding='utf-8')
    print(f"DEBUG: Raw config content length: {len(config_content)} characters")
    
    _raw_config: Dict[str, Any] = json.loads(config_content)
    
    # Debug: Print loaded config (excluding password)
    debug_config = {k: v if k != 'dbpassword' else '***' for k, v in _raw_config.items()}
    print(f"DEBUG: Loaded config values: {debug_config}")
    
    # Validate required fields
    required_keys = {'dbserver', 'dbport', 'dbuser', 'dbpassword', 'dbschema', 'db_tblprefix'}

    # if the config file is missing any required keys, raise an error
    if missing := required_keys - _raw_config.keys():
        raise ValueError(f"Missing config keys: {missing}")

    # Expose as module-level constants
    DBSERVER: str = _raw_config['dbserver']
    DBPORT: int = _raw_config['dbport']
    DBUSER: str = _raw_config['dbuser']
    DBPASSWORD: str = _raw_config['dbpassword']
    DBSCHEMA: str = _raw_config['dbschema']
    DB_TBLPREFIX: str = _raw_config['db_tblprefix']

    # Debug: Print the actual values being used
    print(f"DEBUG: Final values - DBSERVER: {DBSERVER}, DBUSER: {DBUSER}, DBSCHEMA: {DBSCHEMA}")

    # Optional: Keep raw config available
    CONFIG_DICT: Dict[str, Any] = _raw_config

# Handle exceptions
except FileNotFoundError:
    raise RuntimeError(f"Config file not found at {_CONFIG_FILE}")
except json.JSONDecodeError as e:
    print(f"DEBUG: JSON decode error in file {_CONFIG_FILE}")
    print(f"DEBUG: File content: {_CONFIG_FILE.read_text(encoding='utf-8')[:200]}...")
    raise RuntimeError(f"Invalid JSON in config: {e}")
except Exception as e:
    raise RuntimeError(f"Failed to load config: {e}")