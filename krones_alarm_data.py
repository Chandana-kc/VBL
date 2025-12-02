"""
Krones ErgoBloc Alarm Data Parser

Parses real alarm data from Krones ErgoBloc L CSV exports
to create realistic alarm simulation patterns.
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, NamedTuple
from dataclasses import dataclass
from enum import Enum
import os

logger = logging.getLogger(__name__)

class AlarmType(Enum):
    WARNING = "Warning"
    FAULT = "Fault" 
    FIRST_FAULT = "FirstFault"
    NOTE = "Note"
    DEBUG = "Debug"

class Module(Enum):
    MMA = "MMA"  # Main Machine
    BAS = "BAS"  # Basic Systems
    SDC = "SDC"  # Safety/Control
    SBC = "SBC"  # System Block Control
    BCM = "BCM"  # Block Communication

@dataclass
class KronesAlarm:
    """Real Krones alarm structure based on CSV data"""
    module: str
    msg_nr: int
    alarm_type: str
    message: str
    sw_ref: str
    comes_timestamp: datetime
    goes_timestamp: Optional[datetime] = None
    duration_ms: Optional[int] = None
    
    def __post_init__(self):
        if self.goes_timestamp and self.comes_timestamp:
            delta = self.goes_timestamp - self.comes_timestamp
            self.duration_ms = int(delta.total_seconds() * 1000)

class KronesAlarmDataParser:
    """Parser for real Krones ErgoBloc alarm CSV data"""
    
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.alarms: List[KronesAlarm] = []
        self.alarm_patterns: Dict[str, List[KronesAlarm]] = {}
        
    def load_alarm_data(self) -> bool:
        """Load and parse alarm data from CSV file"""
        try:
            if not os.path.exists(self.csv_file_path):
                logger.error(f"CSV file not found: {self.csv_file_path}")
                return False
                
            # Read CSV with proper handling of the complex format
            df = pd.read_csv(self.csv_file_path, low_memory=False)
            
            # Filter for actual alarm history entries
            # Based on CSV structure: AlarmType=10, Modul=34, MsgNr=35, Event=36, SWRef=37
            alarm_rows = df[
                (df.iloc[:, 10].isin(['Warning', 'Fault', 'FirstFault', 'Note', 'Debug'])) &
                (df.iloc[:, 34].notna()) &  # Modul column
                (df.iloc[:, 35].notna()) &  # MsgNr column
                (df.iloc[:, 36].notna())    # Event/Message column
            ]
            
            logger.info(f"Found {len(alarm_rows)} alarm entries in CSV")
            
            # Debug: print first few rows to understand structure
            logger.info("Sample alarm rows:")
            for i, (_, row) in enumerate(alarm_rows.head(3).iterrows()):
                logger.info(f"Row {i}: AlarmType={row.iloc[10]}, Module={row.iloc[34]}, MsgNr={row.iloc[35]}, Message={row.iloc[36]}")
            
            for _, row in alarm_rows.iterrows():
                try:
                    alarm = self._parse_alarm_row(row)
                    if alarm:
                        self.alarms.append(alarm)
                except Exception as e:
                    logger.warning(f"Failed to parse alarm row: {e}")
                    continue
            
            self._build_alarm_patterns()
            logger.info(f"Loaded {len(self.alarms)} alarms with {len(self.alarm_patterns)} patterns")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load alarm data: {e}")
            return False
    
    def _parse_alarm_row(self, row) -> Optional[KronesAlarm]:
        """Parse a single alarm row from CSV"""
        try:
            # Extract timestamps
            comes_str = str(row.iloc[11])  # Comes timestamp
            goes_str = str(row.iloc[19])   # Goes timestamp
            
            if comes_str == 'nan' or not comes_str:
                return None
                
            comes_ts = self._parse_timestamp(comes_str)
            goes_ts = self._parse_timestamp(goes_str) if goes_str != 'nan' else None
            
            # Extract alarm details
            module = str(row.iloc[34])      # Modul
            msg_nr = int(row.iloc[35])      # MsgNr
            alarm_type = str(row.iloc[10])  # AlarmType
            message = str(row.iloc[36])     # Event/Message
            sw_ref = str(row.iloc[37])      # SWRef
            
            return KronesAlarm(
                module=module,
                msg_nr=msg_nr,
                alarm_type=alarm_type,
                message=message,
                sw_ref=sw_ref,
                comes_timestamp=comes_ts,
                goes_timestamp=goes_ts
            )
            
        except Exception as e:
            logger.debug(f"Error parsing alarm row: {e}")
            return None
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp from CSV format"""
        # Handle format: "MM/DD/YYYY HH:MM:SS.mmm"
        try:
            return datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S.%f")
        except ValueError:
            try:
                return datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")
            except ValueError:
                # Fallback to current time if parsing fails
                return datetime.now()
    
    def _build_alarm_patterns(self):
        """Build alarm patterns by module and type"""
        for alarm in self.alarms:
            pattern_key = f"{alarm.module}_{alarm.alarm_type}"
            if pattern_key not in self.alarm_patterns:
                self.alarm_patterns[pattern_key] = []
            self.alarm_patterns[pattern_key].append(alarm)
    
    def get_common_alarms(self) -> Dict[str, List[KronesAlarm]]:
        """Get most common alarm types for simulation"""
        common_patterns = {}
        
        for pattern, alarms in self.alarm_patterns.items():
            if len(alarms) >= 5:  # Only patterns with 5+ occurrences
                common_patterns[pattern] = alarms[:10]  # Limit to top 10
        
        return common_patterns
    
    def get_alarm_by_module(self, module: str) -> List[KronesAlarm]:
        """Get all alarms for a specific module"""
        return [alarm for alarm in self.alarms if alarm.module == module]
    
    def get_cascading_sequences(self) -> List[List[KronesAlarm]]:
        """Identify alarm sequences that might indicate cascading failures"""
        sequences = []
        
        # Sort alarms by timestamp
        sorted_alarms = sorted(self.alarms, key=lambda x: x.comes_timestamp)
        
        current_sequence = []
        last_timestamp = None
        
        for alarm in sorted_alarms:
            if last_timestamp is None:
                current_sequence = [alarm]
            elif (alarm.comes_timestamp - last_timestamp).total_seconds() <= 300:  # 5 minutes
                current_sequence.append(alarm)
            else:
                if len(current_sequence) >= 3:  # Sequence of 3+ alarms
                    sequences.append(current_sequence.copy())
                current_sequence = [alarm]
            
            last_timestamp = alarm.comes_timestamp
        
        # Don't forget the last sequence
        if len(current_sequence) >= 3:
            sequences.append(current_sequence)
        
        return sequences
    
    def get_typical_durations(self) -> Dict[str, Dict[str, float]]:
        """Get typical alarm durations by module and type"""
        duration_stats = {}
        
        for alarm in self.alarms:
            if alarm.duration_ms is not None:
                key = f"{alarm.module}_{alarm.alarm_type}"
                if key not in duration_stats:
                    duration_stats[key] = []
                duration_stats[key].append(alarm.duration_ms)
        
        # Calculate statistics
        stats = {}
        for key, durations in duration_stats.items():
            if durations:
                stats[key] = {
                    'mean': sum(durations) / len(durations),
                    'min': min(durations),
                    'max': max(durations),
                    'count': len(durations)
                }
        
        return stats
    
    def get_sample_alarm_messages(self) -> Dict[str, List[str]]:
        """Get sample alarm messages by module"""
        messages = {}
        
        for alarm in self.alarms:
            if alarm.module not in messages:
                messages[alarm.module] = []
            
            if alarm.message not in messages[alarm.module]:
                messages[alarm.module].append(alarm.message)
        
        return messages

# Example usage and test data
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test with sample CSV path
    csv_path = "/Users/maheshhariharan/Documents/Work/Digital-Manufacturing/VBL/alarms_dump/ss line blow mold and filler todays historical alarm.csv"
    
    parser = KronesAlarmDataParser(csv_path)
    if parser.load_alarm_data():
        print(f"\nLoaded {len(parser.alarms)} alarms")
        
        # Show common alarm patterns
        common = parser.get_common_alarms()
        print(f"\nCommon alarm patterns: {len(common)}")
        for pattern, alarms in common.items():
            print(f"  {pattern}: {len(alarms)} occurrences")
        
        # Show sample messages by module
        messages = parser.get_sample_alarm_messages()
        print(f"\nSample messages by module:")
        for module, msgs in messages.items():
            print(f"  {module}: {len(msgs)} unique messages")
            for msg in msgs[:3]:  # Show first 3
                print(f"    - {msg}")
        
        # Show cascading sequences
        sequences = parser.get_cascading_sequences()
        print(f"\nFound {len(sequences)} cascading alarm sequences")
        
        # Show duration statistics
        durations = parser.get_typical_durations()
        print(f"\nDuration statistics for {len(durations)} alarm types:")
        for alarm_type, stats in list(durations.items())[:5]:
            print(f"  {alarm_type}: avg={stats['mean']:.0f}ms, count={stats['count']}")
