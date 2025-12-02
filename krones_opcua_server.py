"""
Enhanced OPC UA Server for Krones ErgoBloc L with Real Alarm Data

This server simulates a real Krones ErgoBloc L production line using actual alarm data
from CSV exports, providing realistic alarm patterns for Ignition integration.
"""

import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from asyncua import Server, ua
from asyncua.common.node import Node

from krones_alarm_data import KronesAlarmDataParser, KronesAlarm

logger = logging.getLogger(__name__)

# Reduce asyncua logging noise
logging.getLogger("asyncua").setLevel(logging.WARNING)
logging.getLogger("asyncua.server.subscription_service").setLevel(logging.WARNING)
logging.getLogger("asyncua.server.uaprocessor").setLevel(logging.WARNING)

class KronesErgoBlockOPCUAServer:
    """Enhanced OPC UA Server with real Krones alarm data"""
    
    def __init__(self, endpoint: str = "opc.tcp://0.0.0.0:4841/kronesergobloc"):
        self.endpoint = endpoint
        self.server = None
        
        # Real alarm data
        self.alarm_parser = None
        self.current_alarms: List[KronesAlarm] = []
        self.alarm_sequences = []
        
        # OPC UA nodes organized by Krones structure
        self.mma_nodes: Dict[str, Node] = {}     # Main Machine
        self.bas_nodes: Dict[str, Node] = {}     # Basic Systems
        self.sdc_nodes: Dict[str, Node] = {}     # Safety/Control
        self.sbc_nodes: Dict[str, Node] = {}     # System Block Control
        self.bcm_nodes: Dict[str, Node] = {}     # Block Communication
        self.process_nodes: Dict[str, Node] = {} # Process variables
        
        # Simulation state
        self.simulation_running = False
        self.production_rate = 0  # containers per hour
        self.total_production = 0
        
        logger.info(f"Krones ErgoBloc L OPC UA Server initialized for {endpoint}")
    
    async def load_alarm_data(self, csv_path: str) -> bool:
        """Load real alarm data from Krones CSV export"""
        try:
            self.alarm_parser = KronesAlarmDataParser(csv_path)
            success = self.alarm_parser.load_alarm_data()
            
            if success:
                self.alarm_sequences = self.alarm_parser.get_cascading_sequences()
                logger.info(f"Loaded {len(self.alarm_parser.alarms)} alarms and {len(self.alarm_sequences)} sequences")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to load alarm data: {e}")
            return False
    
    async def start_server(self):
        """Start the OPC UA server"""
        self.server = Server()
        await self.server.init()
        
        self.server.set_endpoint(self.endpoint)
        self.server.set_server_name("Krones ErgoBloc L Production Line")
        
        # Set up address space based on real Krones structure
        await self._setup_krones_address_space()
        
        # Start server
        await self.server.start()
        logger.info(f"Krones ErgoBloc L OPC UA Server started at {self.endpoint}")
        
        return self
    
    async def _setup_krones_address_space(self):
        """Set up OPC UA address space matching real Krones ErgoBloc structure"""
        # Root folder
        krones_root = await self.server.nodes.objects.add_folder("ns=2;s=KronesErgoBloc", "Krones ErgoBloc L")
        
        # MMA - Main Machine (Blow Molder + Filler)
        await self._setup_mma_nodes(krones_root)
        
        # BAS - Basic Systems
        await self._setup_bas_nodes(krones_root)
        
        # SDC - Safety/Control
        await self._setup_sdc_nodes(krones_root)
        
        # SBC - System Block Control
        await self._setup_sbc_nodes(krones_root)
        
        # BCM - Block Communication
        await self._setup_bcm_nodes(krones_root)
        
        # Process Variables
        await self._setup_process_nodes(krones_root)
        
        # Add tags from vbltags.json
        import os, json
        vbltags_path = os.path.join(os.path.dirname(__file__), "vbltags.json")
        if os.path.exists(vbltags_path):
            with open(vbltags_path, "r", encoding="utf-8") as f:
                vbltags = json.load(f)
            await self._add_tags_from_json(krones_root, vbltags)
            logger.info("Added tags from vbltags.json to OPC UA address space.")
        else:
            logger.warning(f"vbltags.json not found at {vbltags_path}")
        logger.info("Krones ErgoBloc L address space configured")
        async def _add_tags_from_json(self, parent_node, tag_json):
            """
            Recursively add folders and variables from vbltags.json to the OPC UA address space.
            """
            tag_type = tag_json.get("tagType")
            name = tag_json.get("name")
            if tag_type == "Folder":
                # Create folder node
                folder_node = await parent_node.add_folder(f"ns=2;s={name}", name)
                for child in tag_json.get("tags", []):
                    await self._add_tags_from_json(folder_node, child)
            elif tag_type == "AtomicTag":
                # Create variable node
                data_type = tag_json.get("dataType", "String")
                value = tag_json.get("value", tag_json.get("defaultValue", None))
                ua_type = self._map_json_type_to_ua(data_type)
                var_node = await parent_node.add_variable(f"ns=2;s={name}", name, value, varianttype=ua_type)
                await var_node.set_writable()
            # else: ignore unknown types

        def _map_json_type_to_ua(self, json_type):
            """
            Map JSON dataType to asyncua VariantType
            """
            from asyncua import ua
            type_map = {
                "Float8": ua.VariantType.Double,
                "Int4": ua.VariantType.Int32,
                "String": ua.VariantType.String,
                "Boolean": ua.VariantType.Boolean,
            }
            return type_map.get(json_type, ua.VariantType.String)
    
    async def _setup_mma_nodes(self, parent_folder):
        """Setup Main Machine (MMA) nodes"""
        mma_folder = await parent_folder.add_folder("ns=2;s=MMA", "Main Machine Assembly")
        
        # Machine state and production
        self.mma_nodes = {
            "State": await mma_folder.add_variable("ns=2;s=MMA.State", "Machine State", "STOPPED"),
            "Speed_CPH": await mma_folder.add_variable("ns=2;s=MMA.Speed", "Production Speed", 0),
            "Temperature": await mma_folder.add_variable("ns=2;s=MMA.Temperature", "Temperature", 20.0),
            "Pressure": await mma_folder.add_variable("ns=2;s=MMA.Pressure", "System Pressure", 6.0),
            "Total_Production": await mma_folder.add_variable("ns=2;s=MMA.TotalProduction", "Total Production", 0),
            
            # Real MMA alarms from CSV data
            "Fault_Routine_Active": await mma_folder.add_variable("ns=2;s=MMA.FaultRoutine", "Fault Routine Started", False),
            "Manual_Override": await mma_folder.add_variable("ns=2;s=MMA.ManualOverride", "Manual Override Active", False),
            "Air_Dehumidifier": await mma_folder.add_variable("ns=2;s=MMA.AirDehumidifier", "Air Dehumidifier Ready", True),
            "Guard_Door_1": await mma_folder.add_variable("ns=2;s=MMA.GuardDoor1", "Guard Door 1 Closed", True),
            "Level_LT100": await mma_folder.add_variable("ns=2;s=MMA.LevelLT100", "Level LT100", 50.0),
            "Container_Transfer": await mma_folder.add_variable("ns=2;s=MMA.ContainerTransfer", "Container Transfer OK", True),
            "Cap_Feed_Unit": await mma_folder.add_variable("ns=2;s=MMA.CapFeedUnit", "Cap Feed Unit Ready", True),
            
            # Alarm counters
            "Active_Alarms": await mma_folder.add_variable("ns=2;s=MMA.ActiveAlarms", "Active Alarm Count", 0),
            "Warning_Count": await mma_folder.add_variable("ns=2;s=MMA.WarningCount", "Warning Count", 0),
            "Fault_Count": await mma_folder.add_variable("ns=2;s=MMA.FaultCount", "Fault Count", 0),
            
            # New tags for scenarios
            "MotorProtector_100": await mma_folder.add_variable("ns=2;s=MMA.MotorProtector100", "Motor Protector 100", False),
            "MotorProtector_101": await mma_folder.add_variable("ns=2;s=MMA.MotorProtector101", "Motor Protector 101", False),
            "MotorProtector_103": await mma_folder.add_variable("ns=2;s=MMA.MotorProtector103", "Motor Protector 103", False),
            "ESTOP_Triggered": await mma_folder.add_variable("ns=2;s=MMA.ESTOPTriggered", "ESTOP Triggered", False),
            "LevelTooHigh_LT100": await mma_folder.add_variable("ns=2;s=MMA.LevelTooHighLT100", "Level Too High LT100", False),
            "GuardDoorOpen_1": await mma_folder.add_variable("ns=2;s=MMA.GuardDoorOpen1", "Guard Door Open 1", False),
            "OperatorPanelAccess": await mma_folder.add_variable("ns=2;s=MMA.OperatorPanelAccess", "Operator Panel Access", False),
            "GuardDoorReset": await mma_folder.add_variable("ns=2;s=MMA.GuardDoorReset", "Guard Door Reset", False),
        }
        
        # Set all variables as writable
        for node in self.mma_nodes.values():
            await node.set_writable()
    
    async def _setup_bas_nodes(self, parent_folder):
        """Setup Basic Systems (BAS) nodes"""
        bas_folder = await parent_folder.add_folder("ns=2;s=BAS", "Basic Systems")
        
        self.bas_nodes = {
            "BCM_Server_Status": await bas_folder.add_variable("ns=2;s=BAS.BCMServer", "BCM Server Status", "ONLINE"),
            "Communication_Health": await bas_folder.add_variable("ns=2;s=BAS.CommHealth", "Communication Health", 100),
            "Network_Errors": await bas_folder.add_variable("ns=2;s=BAS.NetworkErrors", "Network Error Count", 0),
            
            # New tags for scenarios
            "PowerLoss": await bas_folder.add_variable("ns=2;s=BAS.PowerLoss", "Power Loss", False),
            "ProfibusFault": await bas_folder.add_variable("ns=2;s=BAS.ProfibusFault", "Profibus Fault", False),
        }
        
        for node in self.bas_nodes.values():
            await node.set_writable()
    
    async def _setup_sdc_nodes(self, parent_folder):
        """Setup Safety/Control (SDC) nodes"""
        sdc_folder = await parent_folder.add_folder("ns=2;s=SDC", "Safety & Control")
        
        self.sdc_nodes = {
            "Power_Supply_Status": await sdc_folder.add_variable("ns=2;s=SDC.PowerSupply", "Power Supply Status", "OK"),
            "Servo_Drive_Status": await sdc_folder.add_variable("ns=2;s=SDC.ServoDrive", "Servo Drive Status", "OK"),
            "Service_Brake_Torque": await sdc_folder.add_variable("ns=2;s=SDC.BrakeTorque", "Service Brake Torque", 100.0),
            "Safety_Circuit": await sdc_folder.add_variable("ns=2;s=SDC.SafetyCircuit", "Safety Circuit OK", True),
        }
        
        for node in self.sdc_nodes.values():
            await node.set_writable()
    
    async def _setup_sbc_nodes(self, parent_folder):
        """Setup System Block Control (SBC) nodes"""
        sbc_folder = await parent_folder.add_folder("ns=2;s=SBC", "System Block Control")
        
        self.sbc_nodes = {
            "Stretching_Drive_12": await sbc_folder.add_variable("ns=2;s=SBC.StretchDrive12", "Stretch Drive Station 12", 0.0),
            "Stretching_Drive_13": await sbc_folder.add_variable("ns=2;s=SBC.StretchDrive13", "Stretch Drive Station 13", 0.0),
            "Stretching_Drive_14": await sbc_folder.add_variable("ns=2;s=SBC.StretchDrive14", "Stretch Drive Station 14", 0.0),
            "Position_Deviation": await sbc_folder.add_variable("ns=2;s=SBC.PositionDev", "Position Deviation", 0.0),
        }
        
        for node in self.sbc_nodes.values():
            await node.set_writable()
    
    async def _setup_bcm_nodes(self, parent_folder):
        """Setup Block Communication (BCM) nodes"""
        bcm_folder = await parent_folder.add_folder("ns=2;s=BCM", "Block Communication")
        
        self.bcm_nodes = {
            "Server_Connection": await bcm_folder.add_variable("ns=2;s=BCM.ServerConn", "Server Connection", "CONNECTED"),
            "Data_Exchange_Rate": await bcm_folder.add_variable("ns=2;s=BCM.DataRate", "Data Exchange Rate", 1000),
            "Message_Queue": await bcm_folder.add_variable("ns=2;s=BCM.MessageQueue", "Message Queue Size", 0),
        }
        
        for node in self.bcm_nodes.values():
            await node.set_writable()
    
    async def _setup_process_nodes(self, parent_folder):
        """Setup Process Variables"""
        process_folder = await parent_folder.add_folder("ns=2;s=Process", "Process Variables")
        self.process_nodes = {
            # Set Line_State to RUNNING by default
            "Line_State": await process_folder.add_variable("ns=2;s=Process.LineState", "Line State", "RUNNING"),
            "OEE": await process_folder.add_variable("ns=2;s=Process.OEE", "Overall Equipment Effectiveness", 0.0),
            "Availability": await process_folder.add_variable("ns=2;s=Process.Availability", "Availability", 0.0),
            "Performance": await process_folder.add_variable("ns=2;s=Process.Performance", "Performance", 0.0),
            "Quality": await process_folder.add_variable("ns=2;s=Process.Quality", "Quality", 0.0),
            "Downtime_Minutes": await process_folder.add_variable("ns=2;s=Process.Downtime", "Downtime (minutes)", 0),
            # New tags for scenarios
            "TriggerScenarioA": await process_folder.add_variable("ns=2;s=Process.TriggerScenarioA", "Trigger Scenario A", False),
            "TriggerScenarioB": await process_folder.add_variable("ns=2;s=Process.TriggerScenarioB", "Trigger Scenario B", False),
        }
        for node in self.process_nodes.values():
            await node.set_writable()
    
    async def run_simulation(self):
        """Run the main simulation with real alarm patterns"""
        if not self.alarm_parser:
            logger.error("No alarm data loaded. Cannot run simulation.")
            return
        
        self.simulation_running = True
        logger.info("Starting Krones ErgoBloc L simulation with real alarm data")
        
        # Start simulation tasks
        await asyncio.gather(
            self._simulate_production(),
            self._simulate_real_alarms(),
            self._update_process_variables()
        )
    
    async def _simulate_production(self):
        """Simulate production with realistic patterns"""
        base_production_rate = 18000  # containers per hour
        
        while self.simulation_running:
            try:
                # Vary production based on current conditions
                efficiency_factor = 1.0
                
                # Reduce production if alarms are active
                if len(self.current_alarms) > 0:
                    efficiency_factor = max(0.2, 1.0 - (len(self.current_alarms) * 0.1))
                
                # Set production rate
                self.production_rate = int(base_production_rate * efficiency_factor)
                await self.mma_nodes["Speed_CPH"].write_value(self.production_rate)
                
                # Update total production
                self.total_production += max(0, self.production_rate / 3600)  # per second
                await self.mma_nodes["Total_Production"].write_value(int(self.total_production))
                
                # Set machine state based on conditions
                # Only set Line_State to FAULT if triggered by scenario, not by alarm count
                if len([a for a in self.current_alarms if 'Fault' in a.alarm_type]) > 0:
                    await self.mma_nodes["State"].write_value("FAULT")
                    # Do NOT set Line_State to FAULT here; scenario logic will handle STOPPED/FAULT
                elif len(self.current_alarms) > 0:
                    await self.mma_nodes["State"].write_value("WARNING")
                    # Do NOT set Line_State to WARNING here
                elif self.production_rate > 1000:
                    await self.mma_nodes["State"].write_value("RUNNING")
                    await self.process_nodes["Line_State"].write_value("RUNNING")
                else:
                    await self.mma_nodes["State"].write_value("STOPPED")
                    await self.process_nodes["Line_State"].write_value("STOPPED")
                
                await asyncio.sleep(1.0)  # Update every second
                
            except Exception as e:
                logger.error(f"Error in production simulation: {e}")
                await asyncio.sleep(5.0)
    
    async def _simulate_real_alarms(self):
        """Simulate alarms using real patterns from CSV data - Enhanced for Ignition demo"""
        import time
        scenario_interval = 30  # seconds (was 120)
        last_scenario_time = time.monotonic()
        scenario_toggle = False  # False: Scenario A, True: Scenario B
        while self.simulation_running:
            try:
                now = time.monotonic()
                # Trigger scenario every 30 seconds
                if (now - last_scenario_time) > scenario_interval:
                    if not scenario_toggle:
                        logger.info("[SCENARIO] Triggering Scenario A (Alarm Flood)")
                        await self._trigger_scenario_a()
                    else:
                        logger.info("[SCENARIO] Triggering Scenario B (Fault Masking)")
                        await self._trigger_scenario_b()
                    scenario_toggle = not scenario_toggle
                    last_scenario_time = now
                # Always stream normal alarms and productivity data
                await self._trigger_alarm_sequence()
                if self.current_alarms:
                    await self._clear_random_alarm()
                await self._trigger_random_alarm()
                await self._update_alarm_counters()
                await asyncio.sleep(0.05)  # 50ms for realistic flood
            except Exception as e:
                logger.error(f"Error in alarm simulation: {e}")
                await asyncio.sleep(2.0)
    
    async def _trigger_alarm_sequence(self):
        """Trigger a realistic alarm sequence from CSV data"""
        if not self.alarm_sequences:
            return
        
        # Select a random sequence
        sequence = random.choice(self.alarm_sequences)
        
        logger.info(f"Triggering alarm sequence with {len(sequence)} alarms")
        
        for i, alarm in enumerate(sequence[:5]):  # Limit to first 5 alarms
            await self._activate_alarm(alarm)
            
            # Delay between alarms in sequence (realistic timing)
            if i < len(sequence) - 1:
                delay = 0.01  # much faster event rate
                await asyncio.sleep(delay)
    
    async def _activate_alarm(self, alarm: KronesAlarm):
        """Activate a specific alarm and update relevant nodes"""
        self.current_alarms.append(alarm)
        tag_names = []
        def log_tag(tag, value, reason):
            logger.info(f"TAG SET: {tag} = {value} ({reason})")
        # Update specific nodes based on alarm content
        if alarm.module == "MMA":
            if "fault routine" in alarm.message.lower():
                await self.mma_nodes["Fault_Routine_Active"].write_value(True)
                log_tag("ns=2;s=MMA.FaultRoutine", True, "Alarm Activated")
                tag_names.append("ns=2;s=MMA.FaultRoutine")
            elif "manual" in alarm.message.lower():
                await self.mma_nodes["Manual_Override"].write_value(True)
                log_tag("ns=2;s=MMA.ManualOverride", True, "Alarm Activated")
                tag_names.append("ns=2;s=MMA.ManualOverride")
            elif "dehumidifier" in alarm.message.lower():
                await self.mma_nodes["Air_Dehumidifier"].write_value(False)
                log_tag("ns=2;s=MMA.AirDehumidifier", False, "Alarm Activated")
                tag_names.append("ns=2;s=MMA.AirDehumidifier")
            elif "guard door" in alarm.message.lower():
                await self.mma_nodes["Guard_Door_1"].write_value(False)
                log_tag("ns=2;s=MMA.GuardDoor1", False, "Alarm Activated")
                tag_names.append("ns=2;s=MMA.GuardDoor1")
            elif "level" in alarm.message.lower() and "LT100" in alarm.message:
                await self.mma_nodes["Level_LT100"].write_value(95.0)
                log_tag("ns=2;s=MMA.LevelLT100", 95.0, "Alarm Activated")
                tag_names.append("ns=2;s=MMA.LevelLT100")
            elif "container transfer" in alarm.message.lower():
                await self.mma_nodes["Container_Transfer"].write_value(False)
                log_tag("ns=2;s=MMA.ContainerTransfer", False, "Alarm Activated")
                tag_names.append("ns=2;s=MMA.ContainerTransfer")
            elif "cap feed" in alarm.message.lower():
                await self.mma_nodes["Cap_Feed_Unit"].write_value(False)
                log_tag("ns=2;s=MMA.CapFeedUnit", False, "Alarm Activated")
                tag_names.append("ns=2;s=MMA.CapFeedUnit")
        elif alarm.module == "BAS":
            if "BCM server" in alarm.message.lower():
                await self.bas_nodes["BCM_Server_Status"].write_value("OFFLINE")
                log_tag("ns=2;s=BAS.BCMServer", "OFFLINE", "Alarm Activated")
                tag_names.append("ns=2;s=BAS.BCMServer")
                await self.bas_nodes["Communication_Health"].write_value(0)
                log_tag("ns=2;s=BAS.CommHealth", 0, "Alarm Activated")
                tag_names.append("ns=2;s=BAS.CommHealth")
        elif alarm.module == "SDC":
            if "power supply" in alarm.message.lower():
                await self.sdc_nodes["Power_Supply_Status"].write_value("FAULT")
                log_tag("ns=2;s=SDC.PowerSupply", "FAULT", "Alarm Activated")
                tag_names.append("ns=2;s=SDC.PowerSupply")
            elif "servo drive" in alarm.message.lower():
                await self.sdc_nodes["Servo_Drive_Status"].write_value("FAULT")
                log_tag("ns=2;s=SDC.ServoDrive", "FAULT", "Alarm Activated")
                tag_names.append("ns=2;s=SDC.ServoDrive")
            elif "brake" in alarm.message.lower():
                await self.sdc_nodes["Service_Brake_Torque"].write_value(50.0)
                log_tag("ns=2;s=SDC.BrakeTorque", 50.0, "Alarm Activated")
                tag_names.append("ns=2;s=SDC.BrakeTorque")
        elif alarm.module == "SBC":
            if "stretching drive" in alarm.message.lower():
                deviation = random.uniform(5.0, 15.0)
                await self.sbc_nodes["Position_Deviation"].write_value(deviation)
                log_tag("ns=2;s=SBC.PositionDev", deviation, "Alarm Activated")
                tag_names.append("ns=2;s=SBC.PositionDev")
                if "station: 12" in alarm.message:
                    await self.sbc_nodes["Stretching_Drive_12"].write_value(deviation)
                    log_tag("ns=2;s=SBC.StretchDrive12", deviation, "Alarm Activated")
                    tag_names.append("ns=2;s=SBC.StretchDrive12")
                elif "station: 13" in alarm.message:
                    await self.sbc_nodes["Stretching_Drive_13"].write_value(deviation)
                    log_tag("ns=2;s=SBC.StretchDrive13", deviation, "Alarm Activated")
                    tag_names.append("ns=2;s=SBC.StretchDrive13")
                elif "station: 14" in alarm.message:
                    await self.sbc_nodes["Stretching_Drive_14"].write_value(deviation)
                    log_tag("ns=2;s=SBC.StretchDrive14", deviation, "Alarm Activated")
                    tag_names.append("ns=2;s=SBC.StretchDrive14")
        logger.info(f"Activated {alarm.module} {alarm.alarm_type}: {alarm.message[:50]} | Tags: {', '.join(tag_names)}")
    
    async def _clear_random_alarm(self):
        """Clear a random active alarm"""
        if not self.current_alarms:
            return
        
        alarm = random.choice(self.current_alarms)
        self.current_alarms.remove(alarm)
        tag_names = []
        def log_tag(tag, value, reason):
            logger.info(f"TAG SET: {tag} = {value} ({reason})")
        # Reset corresponding nodes to normal values
        if alarm.module == "MMA":
            if "fault routine" in alarm.message.lower():
                await self.mma_nodes["Fault_Routine_Active"].write_value(False)
                log_tag("ns=2;s=MMA.FaultRoutine", False, "Alarm Cleared")
                tag_names.append("ns=2;s=MMA.FaultRoutine")
            elif "manual" in alarm.message.lower():
                await self.mma_nodes["Manual_Override"].write_value(False)
                log_tag("ns=2;s=MMA.ManualOverride", False, "Alarm Cleared")
                tag_names.append("ns=2;s=MMA.ManualOverride")
            elif "dehumidifier" in alarm.message.lower():
                await self.mma_nodes["Air_Dehumidifier"].write_value(True)
                log_tag("ns=2;s=MMA.AirDehumidifier", True, "Alarm Cleared")
                tag_names.append("ns=2;s=MMA.AirDehumidifier")
            elif "guard door" in alarm.message.lower():
                await self.mma_nodes["Guard_Door_1"].write_value(True)
                log_tag("ns=2;s=MMA.GuardDoor1", True, "Alarm Cleared")
                tag_names.append("ns=2;s=MMA.GuardDoor1")
            elif "level" in alarm.message.lower() and "LT100" in alarm.message:
                await self.mma_nodes["Level_LT100"].write_value(50.0)
                log_tag("ns=2;s=MMA.LevelLT100", 50.0, "Alarm Cleared")
                tag_names.append("ns=2;s=MMA.LevelLT100")
            elif "container transfer" in alarm.message.lower():
                await self.mma_nodes["Container_Transfer"].write_value(True)
                log_tag("ns=2;s=MMA.ContainerTransfer", True, "Alarm Cleared")
                tag_names.append("ns=2;s=MMA.ContainerTransfer")
            elif "cap feed" in alarm.message.lower():
                await self.mma_nodes["Cap_Feed_Unit"].write_value(True)
                log_tag("ns=2;s=MMA.CapFeedUnit", True, "Alarm Cleared")
                tag_names.append("ns=2;s=MMA.CapFeedUnit")
        elif alarm.module == "BAS":
            await self.bas_nodes["BCM_Server_Status"].write_value("ONLINE")
            log_tag("ns=2;s=BAS.BCMServer", "ONLINE", "Alarm Cleared")
            tag_names.append("ns=2;s=BAS.BCMServer")
            await self.bas_nodes["Communication_Health"].write_value(100)
            log_tag("ns=2;s=BAS.CommHealth", 100, "Alarm Cleared")
            tag_names.append("ns=2;s=BAS.CommHealth")
        elif alarm.module == "SDC":
            await self.sdc_nodes["Power_Supply_Status"].write_value("OK")
            log_tag("ns=2;s=SDC.PowerSupply", "OK", "Alarm Cleared")
            tag_names.append("ns=2;s=SDC.PowerSupply")
            await self.sdc_nodes["Servo_Drive_Status"].write_value("OK")
            log_tag("ns=2;s=SDC.ServoDrive", "OK", "Alarm Cleared")
            tag_names.append("ns=2;s=SDC.ServoDrive")
            await self.sdc_nodes["Service_Brake_Torque"].write_value(100.0)
            log_tag("ns=2;s=SDC.BrakeTorque", 100.0, "Alarm Cleared")
            tag_names.append("ns=2;s=SDC.BrakeTorque")
        elif alarm.module == "SBC":
            await self.sbc_nodes["Position_Deviation"].write_value(0.0)
            log_tag("ns=2;s=SBC.PositionDev", 0.0, "Alarm Cleared")
            tag_names.append("ns=2;s=SBC.PositionDev")
            await self.sbc_nodes["Stretching_Drive_12"].write_value(0.0)
            log_tag("ns=2;s=SBC.StretchDrive12", 0.0, "Alarm Cleared")
            tag_names.append("ns=2;s=SBC.StretchDrive12")
            await self.sbc_nodes["Stretching_Drive_13"].write_value(0.0)
            log_tag("ns=2;s=SBC.StretchDrive13", 0.0, "Alarm Cleared")
            tag_names.append("ns=2;s=SBC.StretchDrive13")
            await self.sbc_nodes["Stretching_Drive_14"].write_value(0.0)
            log_tag("ns=2;s=SBC.StretchDrive14", 0.0, "Alarm Cleared")
            tag_names.append("ns=2;s=SBC.StretchDrive14")
        logger.info(f"Cleared {alarm.module} alarm: {alarm.message[:50]} | Tags: {', '.join(tag_names)}")
    
    async def _trigger_random_alarm(self):
        """Trigger a random individual alarm for dynamic demo"""
        if not self.alarm_parser or not self.alarm_parser.alarms:
            return
        
        # Select a random alarm from the dataset
        alarm = random.choice(self.alarm_parser.alarms)
        await self._activate_alarm(alarm)
        
        logger.info(f"Triggered random {alarm.module} {alarm.alarm_type}")

    async def _update_alarm_counters(self):
        """Update alarm counter nodes"""
        total_alarms = len(self.current_alarms)
        warning_count = len([a for a in self.current_alarms if a.alarm_type == "Warning"])
        fault_count = len([a for a in self.current_alarms if "Fault" in a.alarm_type])
        
        await self.mma_nodes["Active_Alarms"].write_value(total_alarms)
        await self.mma_nodes["Warning_Count"].write_value(warning_count)
        await self.mma_nodes["Fault_Count"].write_value(fault_count)
    
    async def _update_process_variables(self):
        """Update process variables like OEE"""
        while self.simulation_running:
            try:
                # Calculate OEE components
                availability = max(0.0, 1.0 - (len(self.current_alarms) * 0.1))
                performance = self.production_rate / 18000.0 if self.production_rate > 0 else 0.0
                quality = max(0.85, 1.0 - (len([a for a in self.current_alarms if "Fault" in a.alarm_type]) * 0.05))
                
                oee = availability * performance * quality
                
                # Update nodes
                await self.process_nodes["Availability"].write_value(round(availability * 100, 1))
                await self.process_nodes["Performance"].write_value(round(performance * 100, 1))
                await self.process_nodes["Quality"].write_value(round(quality * 100, 1))
                await self.process_nodes["OEE"].write_value(round(oee * 100, 1))
                
                # Update temperature and pressure with some variation
                temp = 20.0 + random.uniform(-2, 8)  # 18-28°C
                pressure = 6.0 + random.uniform(-0.5, 1.0)  # 5.5-7.0 bar
                
                await self.mma_nodes["Temperature"].write_value(round(temp, 1))
                await self.mma_nodes["Pressure"].write_value(round(pressure, 1))
                
                await asyncio.sleep(5.0)  # Update every 5 seconds
                
            except Exception as e:
                logger.error(f"Error updating process variables: {e}")
                await asyncio.sleep(10.0)
    
    async def _trigger_scenario_a(self):
        """Simulate Scenario A: Cascading System Fault (Alarm Flood)"""
        await self.process_nodes["Line_State"].write_value("STOPPED")
        logger.info("Line_State set to STOPPED for Scenario A")
        await self.mma_nodes["MotorProtector_100"].write_value(True)
        await asyncio.sleep(0.005)
        await self.mma_nodes["MotorProtector_101"].write_value(True)
        await asyncio.sleep(0.005)
        await self.mma_nodes["MotorProtector_103"].write_value(True)
        await asyncio.sleep(0.005)
        await self.mma_nodes["ESTOP_Triggered"].write_value(True)
        await asyncio.sleep(0.005)
        await self.bas_nodes["PowerLoss"].write_value(True)
        await asyncio.sleep(0.005)
        await self.bas_nodes["ProfibusFault"].write_value(True)
        # Dwell for 10 seconds (was 30)
        await asyncio.sleep(10)
        await self.mma_nodes["MotorProtector_100"].write_value(False)
        await self.mma_nodes["MotorProtector_101"].write_value(False)
        await self.mma_nodes["MotorProtector_103"].write_value(False)
        await self.mma_nodes["ESTOP_Triggered"].write_value(False)
        await self.bas_nodes["PowerLoss"].write_value(False)
        await self.bas_nodes["ProfibusFault"].write_value(False)
        logger.info("Scenario A (Alarm Flood) triggered.")
        await self.process_nodes["Line_State"].write_value("RUNNING")
        logger.info("Line_State set to RUNNING after Scenario A")

    async def _trigger_scenario_b(self):
        """Simulate Scenario B: Fault Masking (Operator Intervention)"""
        await self.process_nodes["Line_State"].write_value("STOPPED")
        logger.info("Line_State set to STOPPED for Scenario B")
        await self.mma_nodes["LevelTooHigh_LT100"].write_value(True)
        await asyncio.sleep(0.005)
        await self.mma_nodes["GuardDoorOpen_1"].write_value(True)
        await asyncio.sleep(0.005)
        await self.mma_nodes["OperatorPanelAccess"].write_value(True)
        await asyncio.sleep(0.005)
        await self.mma_nodes["GuardDoorReset"].write_value(True)
        # Dwell for 10 seconds (was 30)
        await asyncio.sleep(10)
        await self.mma_nodes["LevelTooHigh_LT100"].write_value(False)
        await self.mma_nodes["GuardDoorOpen_1"].write_value(False)
        await self.mma_nodes["OperatorPanelAccess"].write_value(False)
        await self.mma_nodes["GuardDoorReset"].write_value(False)
        logger.info("Scenario B (Fault Masking) triggered.")
        await self.process_nodes["Line_State"].write_value("RUNNING")
        logger.info("Line_State set to RUNNING after Scenario B")
    
    async def stop_server(self):
        """Stop the OPC UA server"""
        self.simulation_running = False
        if self.server:
            await self.server.stop()
            logger.info("Krones ErgoBloc L OPC UA Server stopped")

# Main execution
async def main():
    """Main function to run the enhanced OPC UA server"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and configure server
    server = KronesErgoBlockOPCUAServer()
    
    # Try multiple paths for alarm data
    csv_paths = [
        "/app/alarms_dump/ss line blow mold and filler todays historical alarm.csv",  # Docker path
        "/Users/maheshhariharan/Documents/Work/Digital-Manufacturing/VBL/alarms_dump/ss line blow mold and filler todays historical alarm.csv",  # Local path
        "./alarms_dump/ss line blow mold and filler todays historical alarm.csv"  # Relative path
    ]
    
    alarm_data_loaded = False
    for csv_path in csv_paths:
        if await server.load_alarm_data(csv_path):
            logger.info(f"Successfully loaded alarm data from: {csv_path}")
            alarm_data_loaded = True
            break
        else:
            logger.warning(f"Could not load alarm data from: {csv_path}")
    
    if not alarm_data_loaded:
        logger.warning("No alarm data loaded. Starting basic simulation without real alarm patterns.")
    
    try:
        # Start server
        await server.start_server()
        
        # Run simulation (will work with or without alarm data)
        if alarm_data_loaded:
            await server.run_simulation()
        else:
            # Basic simulation without alarm data
            logger.info("Running basic simulation without alarm patterns")
            while True:
                await asyncio.sleep(30)
                logger.info("OPC UA Server running - waiting for Ignition connection...")
        
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Server error: {e}")
    finally:
        await server.stop_server()

if __name__ == "__main__":
    asyncio.run(main())
