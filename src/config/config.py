import json
from pathlib import Path
from typing import Any, Dict

# find_config function to search for .conf file in parent directories
def find_config( start_path: Path = Path( __file__ ) ) -> Path:
    
    # find the path to the config file
    for parent in [start_path, *start_path.parents]:
        if ( parent / '.kptvconf' ).exists( ):
            return parent / '.kptvconf'
        
    # If not found, raise an error
    raise FileNotFoundError( "Could not find .kptvconf in any parent directory" )

# hold the config file
_CONFIG_FILE = find_config( )

# Check if the config file exists and is readable
try:
    
    # Load and parse the config file
    _raw_config: Dict[str, Any] = json.loads( _CONFIG_FILE.read_text( encoding='utf-8' ) )
    
    # Validate required fields
    required_keys = {'dbserver', 'dbport', 'dbuser', 'dbpassword', 'dbschema', 'db_tblprefix'}

    # if the config file is missing any required keys, raise an error
    if missing := required_keys - _raw_config.keys( ):
        raise ValueError( f"Missing config keys: {missing}" )

    # Expose as module-level constants
    DBSERVER: str = _raw_config['dbserver']
    DBPORT: int = _raw_config['dbport']
    DBUSER: str = _raw_config['dbuser']
    DBPASSWORD: str = _raw_config['dbpassword']
    DBSCHEMA: str = _raw_config['dbschema']
    DB_TBLPREFIX: str = _raw_config['db_tblprefix']

    # Optional: Keep raw config available
    CONFIG_DICT: Dict[str, Any] = _raw_config

# Handle exceptions
except FileNotFoundError:
    raise RuntimeError( f"Config file not found at {_CONFIG_FILE}" )
except json.JSONDecodeError as e:
    raise RuntimeError( f"Invalid JSON in config: {e}" )
except Exception as e:
    raise RuntimeError( f"Failed to load config: {e}" )
