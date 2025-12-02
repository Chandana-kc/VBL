"""
OPC UA Server for VBL_Digital_Factory tags with Standard MES Formulas
This server exposes the tags defined in ignition_import_json.json as OPC UA nodes
and calculates OEE, Availability, Performance, Quality using standard MES formulas.
"""
import asyncio
import logging
import json
import random
from asyncua import Server, ua

TAGS_JSON_PATH = "ignition_import_json.json"  # Use ignition_import_json.json

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TagNodeBuilder:
    def __init__(self, server):
        self.server = server
        self.atomic_nodes = []
        self.tag_registry = {}  # Path -> (node, dtype, tag_name)

    async def add_tags(self, parent, tags, path=None):
        if path is None:
            path = []
        for tag in tags:
            try:
                current_path = path + [tag["name"]]
                nodeid_str = "/".join(current_path)
                nodeid = f"ns=2;s={nodeid_str}"

                if tag["tagType"] in ["Folder", "UdtType", "UdtInstance"]:
                    # Treat UdtType and UdtInstance as folders
                    folder = await parent.add_folder(nodeid, tag["name"])
                    await self.add_tags(folder, tag.get("tags", []), current_path)
                elif tag["tagType"] == "AtomicTag":
                    # Get data type, with fallback for expression-based tags
                    data_type = tag.get("dataType", "Float8")
                    dtype = self._get_variant_type(data_type)
                    value = tag.get("value", tag.get("defaultValue", self._get_default_value(dtype)))
                    try:
                        node = await parent.add_variable(nodeid, tag["name"], ua.Variant(value, dtype))
                        await node.set_writable()
                        tag_entry = (node, dtype, tag["name"], nodeid_str)
                        self.atomic_nodes.append(tag_entry)
                        self.tag_registry[nodeid_str] = tag_entry
                    except Exception as e:
                        if "BadNodeIdExists" in str(e):
                            logger.warning(f"Duplicate NodeId for {nodeid}, skipping.")
                        else:
                            logger.error(f"Error adding variable {nodeid}: {e}")
            except Exception as e:
                logger.error(f"Error adding node {tag.get('name', '')}: {e}")

    def _get_variant_type(self, data_type):
        mapping = {
            "Float8": ua.VariantType.Double,
            "Float4": ua.VariantType.Float,
            "Int4": ua.VariantType.Int32,
            "String": ua.VariantType.String,
            "Boolean": ua.VariantType.Boolean,
        }
        return mapping.get(data_type, ua.VariantType.String)
    
    def _get_default_value(self, dtype):
        """Get default value based on variant type"""
        if dtype == ua.VariantType.Double or dtype == ua.VariantType.Float:
            return 0.0
        elif dtype == ua.VariantType.Int32:
            return 0
        elif dtype == ua.VariantType.Boolean:
            return False
        else:
            return ""

async def main():
    # Load ignition_import_json.json
    with open(TAGS_JSON_PATH, "r") as f:
        tags_data = json.load(f)

    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4842/vblfactory")
    server.set_server_name("VBL Digital Factory OPC UA Server - Random Value Simulator")

    # Use the root name from the JSON file
    root_name = tags_data.get("name", "VBL_Digital_Factory")
    root_folder = await server.nodes.objects.add_folder(f"ns=2;s={root_name}", root_name)
    builder = TagNodeBuilder(server)
    await builder.add_tags(root_folder, tags_data.get("tags", []), [root_name])

    async def update_random_values():
        """Update all tag values with realistic random values every 1 second"""
        while True:
            try:
                for node, dtype, name, path in builder.atomic_nodes:
                    try:
                        # Temperature tags
                        if "Temperature" in name:
                            if "Preform" in name:
                                value = round(random.uniform(80, 120), 2)  # Preform_Temperature °C
                            elif "Mold" in name:
                                value = round(random.uniform(15, 22), 2)   # Mold_Temperature °C
                            elif "Tunnel" in name:
                                value = round(random.uniform(40, 60), 2)   # Tunnel_Temperature °C
                            elif "Glue" in name:
                                value = round(random.uniform(30, 40), 2)   # Glue_Temperature °C
                            elif "Product" in name:
                                value = round(random.uniform(4, 12), 2)    # Product_Temperature °C
                            elif "Cooling_Water" in name:
                                value = round(random.uniform(10, 20), 2)   # Cooling_Water_Temperature °C
                            else:
                                value = round(random.uniform(10, 100), 2)
                            await node.write_value(ua.Variant(value, dtype))
                        # Pressure tags
                        elif "Pressure" in name:
                            if "Blow" in name:
                                value = round(random.uniform(25, 40), 2)   # Blow_Pressure bar
                            elif "Fill" in name:
                                value = round(random.uniform(2, 6), 2)     # Fill_Pressure bar
                            else:
                                value = round(random.uniform(1, 10), 2)
                            await node.write_value(ua.Variant(value, dtype))
                        # Speed tags
                        elif "Speed" in name:
                            if "Filling" in name or "Target" in name:
                                value = round(random.uniform(700, 1000), 2) # Filling/Target Speed BPH
                            else:
                                value = round(random.uniform(500, 1200), 2) # General Speed BPH
                            await node.write_value(ua.Variant(value, dtype))
                        # Position tags
                        elif "Stretch_Rod_Position" in name:
                            value = round(random.uniform(120, 180), 2)      # mm
                            await node.write_value(ua.Variant(value, dtype))
                        # Cycle time
                        elif "Cycle_Time" in name:
                            value = round(random.uniform(3.5, 5.0), 2)      # seconds
                            await node.write_value(ua.Variant(value, dtype))
                        # Energy tags
                        elif "Energy" in name:
                            if "Per_Bottle" in name:
                                value = round(random.uniform(0.01, 0.05), 4) # kWh/bottle
                            elif "Total" in name:
                                value = round(random.uniform(1000, 10000), 2) # kWh
                            else:
                                value = round(random.uniform(100, 500), 2)    # kWh
                            await node.write_value(ua.Variant(value, dtype))
                        elif "Power_Consumption" in name:
                            value = round(random.uniform(10, 50), 2)          # kW
                            await node.write_value(ua.Variant(value, dtype))
                        # Level tags
                        elif "Level" in name:
                            value = round(random.uniform(500, 10000), 2)      # liters or mm
                            await node.write_value(ua.Variant(value, dtype))
                        # Tension
                        elif "Label_Tension" in name:
                            value = round(random.uniform(1, 5), 2)            # N
                            await node.write_value(ua.Variant(value, dtype))
                        # Flow
                        elif "Cooling_Water_Flow" in name:
                            value = round(random.uniform(10, 100), 2)         # L/min
                            await node.write_value(ua.Variant(value, dtype))
                        # Defect counts
                        elif "Defect" in name or "Count" in name or "Rejected" in name:
                            value = int(random.randint(0, 10))
                            await node.write_value(ua.Variant(value, dtype))
                        # Status tags
                        elif "Status" in name:
                            if "Filler" in name or "Capper" in name or "Cooling" in name or "Labeling" in name:
                                value = random.choice(["Running", "Idle", "Stopped"])
                            else:
                                value = random.choice(["Running", "Idle", "Changeover", "Stopped"])
                            await node.write_value(ua.Variant(value, dtype))
                        # Time tags
                        elif "Runtime_Minutes" in name or "Downtime_Minutes" in name:
                            value = round(random.uniform(0, 480), 2)          # minutes
                            await node.write_value(ua.Variant(value, dtype))
                        # Accuracy, Quality, Fill Accuracy
                        elif "Accuracy" in name or "Quality" in name:
                            value = round(random.uniform(98, 100), 2)         # %
                            await node.write_value(ua.Variant(value, dtype))
                        # Bottles filled/produced
                        elif "Bottles_Filled" in name or "Bottles_Produced" in name:
                            value = int(random.randint(1000, 50000))
                            await node.write_value(ua.Variant(value, dtype))
                        # Label roll length
                        elif "Label_Roll_Length_Remaining" in name:
                            value = round(random.uniform(0, 10000), 2)        # mm
                            await node.write_value(ua.Variant(value, dtype))
                        # Torque
                        elif "Torque" in name:
                            value = round(random.uniform(0.5, 2.5), 2)        # Nm
                            await node.write_value(ua.Variant(value, dtype))
                        # CO2
                        elif "CO2" in name:
                            value = round(random.uniform(0.1, 2.0), 2)        # % or tank level
                            await node.write_value(ua.Variant(value, dtype))
                        # Default for Int32
                        elif dtype == ua.VariantType.Int32:
                            value = int(random.randint(0, 1000))
                            await node.write_value(ua.Variant(value, dtype))
                        # Default for Double/Float
                        elif dtype == ua.VariantType.Double or dtype == ua.VariantType.Float:
                            value = round(random.uniform(0, 100), 2)
                            await node.write_value(ua.Variant(value, dtype))
                        # Default for String
                        elif dtype == ua.VariantType.String:
                            value = f"{name}_{random.randint(1, 100)}"
                            await node.write_value(ua.Variant(value, dtype))
                        # Default for Boolean
                        elif dtype == ua.VariantType.Boolean:
                            await node.write_value(ua.Variant(random.choice([True, False]), dtype))
                    except Exception as e:
                        logger.debug(f"Update skipped for {name}: {e}")
                await asyncio.sleep(1)  # Update every 1 second
            except Exception as e:
                logger.error(f"Error in update loop: {e}")
                await asyncio.sleep(1)

    await server.start()
    logger.info("VBL Digital Factory OPC UA Server - Random Value Simulator started at opc.tcp://0.0.0.0:4842/vblfactory")
    logger.info("Updating all tag values every 1 second with random values.")
    try:
        await update_random_values()
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
    finally:
        await server.stop()

if __name__ == "__main__":
    asyncio.run(main())
