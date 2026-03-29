#!/usr/bin/env python3
"""
Script to transform sensor_metadata SQL inserts to tag_metadata format.
Uses equipment tree data and comprehensive mapping to properly map equipment_id and path.
"""

import re
import sys
from datetime import datetime
from pathlib import Path

# Configuration
INPUT_FILE = "insert.sql"  # Your existing file
OUTPUT_FILE = "tag_metadata_insert.sql"  # Output file
EQUIPMENT_TREE_FILE = "equipment_tree.csv"  # Your equipment tree CSV file
DEFAULT_SCAN_RATE = 1000  # Default scan rate in milliseconds
DEFAULT_ENABLED = 1  # Default enabled status

# Comprehensive Equipment ID Mapping
EQUIPMENT_ID_MAPPING = {
    # Boiler equipment (matches your tree)
    'Boiler Drum': 'BOILER-DRUM-001',
    'Boiler Drum (Steam Drum)': 'BOILER-DRUM-001',
    'Feedwater System': 'FEEDWATER-SYS-001',
    'Feedwater': 'FEEDWATER-SYS-001',
    'Furnace': 'FURNACE-001',
    'Furnace / Combustion Chamber': 'FURNACE-001',
    'Superheater': 'SUPERHEATER-001',
    'Economizer': 'ECONOMIZER-001',
    'Air System': 'AIR-SYS-001',
    'Air System (FD Fan / PA Fan)': 'AIR-SYS-001',
    'Induced Draft Fan (ID Fan)': 'ID-FAN-001',
    'ID Fan': 'ID-FAN-001',
    'Fuel System': 'FUEL-SYS-001',
    'Fuel System (Coal / Gas / Oil)': 'FUEL-SYS-001',
    'Steam Output System': 'STEAM-OUT-001',
    'Safety and Protection': 'SAFETY-SYS-001',
    
    # Turbine equipment (matches your tree)
    'HP Turbine': 'HP-TURBINE-001',
    'IP Turbine': 'IP-TURBINE-001',
    'LP Turbine': 'LP-TURBINE-001',
    'Turbine Rotor': 'TURBINE-ROTOR-001',
    'Turbine Bearings': 'TURBINE-BEARINGS-001',
    'Lubrication System': 'LUBE-SYS-001',
    'Steam Valves': 'STEAM-VALVES-001',
    'Turning Gear': 'TURNING-GEAR-001',
    'Gland Seals': 'GLAND-SEALS-001',
    'Turbine Protection': 'TURBINE-PROT-001',
    'Steam Turbine': 'TURBINE-001',
    
    # Generator equipment (matches your tree)
    'Generator': 'GENERATOR-001',
    'Stator': 'STATOR-001',
    'Rotor': 'ROTOR-GEN-001',
    'Exciter': 'EXCITER-001',
    'AVR': 'AVR-001',
    'Generator Bearings': 'GEN-BEARINGS-001',
    'Hydrogen Cooling': 'H2-COOLING-001',
    'Hydrogen Cooling System': 'H2-COOLING-001',
    'H2 Gas Control Panel': 'H2-CONTROL-001',
    'Hydrogen Gas Control Panel': 'H2-CONTROL-001',
    'Seal Oil System': 'SEAL-OIL-001',
    'Stator Cooling Water': 'SCW-SYS-001',
    'Stator Cooling Water System': 'SCW-SYS-001',
    'Generator Transformer': 'GEN-TRANSFORMER-001',
    'Neutral Grounding': 'NEUTRAL-GRD-001',
    
    # Condenser equipment (matches your tree)
    'Condenser': 'CONDENSER-001',
    'Condenser Shell': 'COND-SHELL-001',
    'Condenser Tubes': 'COND-TUBES-001',
    'Water Box': 'WATER-BOX-001',
    'Water Box - Inlet': 'WATER-BOX-IN-001',
    'Water Box - Outlet': 'WATER-BOX-OUT-001',
    'Tube Sheets': 'TUBE-SHEETS-001',
    'Hotwell': 'HOTWELL-001',
    'Air Extraction': 'AIR-EXTRACT-001',
    'Air Extraction Zone': 'AIR-EXTRACT-001',
    'Vacuum Breaking Valve': 'VACUUM-VALVE-001',
    
    # Cooling Tower equipment (matches your tree)
    'Cooling Tower': 'CT-001',
    'Cooling Tower Structure': 'CT-STRUCTURE-001',
    'Fill Material': 'CT-FILL-001',
    'Drift Eliminators': 'CT-DRIFT-ELIM-001',
    'Water Distribution System': 'CT-WATER-DIST-001',
    'Water Distribution': 'CT-WATER-DIST-001',
    'Cold Water Basin': 'CT-BASIN-001',
    'Cooling Tower Basin': 'CT-BASIN-001',
    'CT Fans': 'CT-FAN-001',
    'CT Fan': 'CT-FAN-001',
    'CT Fan Motor': 'CT-FAN-MOTOR-001',
    'CT Gearbox': 'CT-GEARBOX-001',
    'CT Fan Blades': 'CT-FAN-BLADES-001',
    'Make-up Water Valve': 'CT-MAKEUP-VALVE-001',
    'Bleed-off System': 'CT-BLOWDOWN-001',
    'Louvers': 'CT-LOUVERS-001',
    
    # Coal Handling equipment (matches your tree)
    'Coal Handling': 'COAL-HAND-001',
    'Wagon Tippler': 'WAGON-TIPPLER-001',
    'Stacker-Reclaimer': 'STACKER-RECLAIMER-001',
    'Crusher': 'CRUSHER-001',
    'Vibrating Screen': 'VIBRATING-SCREEN-001',
    'Conveyor Belt': 'CONVEYOR-001',
    'Magnetic Separator': 'MAG-SEP-001',
    
    # Ash Handling equipment (matches your tree)
    'Ash Handling': 'ASH-HAND-001',
    'Bottom Ash Hopper': 'BA-HOPPER-001',
    'Clinker Grinder': 'CLINKER-GRINDER-001',
    'Ash Slurry Pump': 'ASH-SLURRY-PUMP-001',
    'Hydro-cyclone': 'HYDRO-CYCLONE-001',
    'Ash Silo': 'ASH-SILO-001',
    'Dust Suppression System': 'DUST-SUPPRESS-001',
    
    # Water Treatment equipment (matches your tree)
    'Water Treatment': 'WATER-TREAT-001',
    'Clarifier': 'CLARIFIER-001',
    'Filter': 'FILTER-001',
    'Filters': 'FILTER-001',
    'DM Plant': 'DM-PLANT-001',
    'Cation Exchanger': 'CATION-EX-001',
    'Anion Exchanger': 'ANION-EX-001',
    'Mixed Bed Exchanger': 'MIXED-BED-001',
    'RO System': 'RO-SYSTEM-001',
    'Degasser Tower': 'DEGASSER-001',
    
    # Electrical System equipment (matches your tree)
    'Electrical System': 'ELECTRICAL-001',
    'Generator Transformer': 'GEN-TRANSFORMER-ELEC-001',
    'Station Transformer': 'STATION-TRANSFORMER-001',
    'Switchyard': 'SWITCHYARD-001',
    'Switchgear': 'SWITCHGEAR-001',
    'HV Switchgear': 'SWITCHGEAR-HV-001',
    'LV Switchgear': 'SWITCHGEAR-LV-001',
    'Motor Control Center': 'MCC-001',
    'MCC': 'MCC-001',
    'Battery Bank': 'BATTERY-BANK-001',
    'Battery Charger': 'BATTERY-CHARGER-001',
    'DC System': 'BATTERY-BANK-001', 
    'DC System (Battery Bank)': 'BATTERY-BANK-001',
    'Emergency Diesel Generator': 'EMERGENCY-DG-001',
    
    # Instrumentation equipment (matches your tree)
    'Instrumentation & Control': 'IC-001',
    'DCS': 'DCS-001',
    'PLC': 'PLC-001',
    'Field Sensors': 'FIELD-SENSORS-001',
    'Pressure Transmitter': 'PRESSURE-TRANS-001',
    'Temperature Transmitter': 'TEMP-TRANS-001',
    'Flow Meter': 'FLOW-METERS-001',
    'Level Transmitter': 'LEVEL-TRANS-001',
    'Actuator': 'ACTUATORS-001',
    'Control Valve': 'CONTROL-VALVES-001',
    'Vibration Monitoring': 'VIBRATION-MON-001',
    'Vibration Sensor': 'VIBRATION-MON-001',
    'Analyzer': 'ANALYZERS-001',
    'Control Room Console': 'CONTROL-ROOM-001',
}

# Kafka topic mapping based on equipment type
KAFKA_TOPIC_MAPPING = {
    # Boiler related
    'Boiler Drum': 'boiler-sensors',
    'Feedwater System': 'boiler-sensors',
    'Furnace': 'boiler-sensors',
    'Superheater': 'boiler-sensors',
    'Economizer': 'boiler-sensors',
    'Air System': 'boiler-sensors',
    'ID Fan': 'boiler-sensors',
    'Fuel System': 'boiler-sensors',
    'Steam Output': 'boiler-sensors',
    'Safety System': 'boiler-sensors',
    
    # Turbine related
    'HP Turbine': 'turbine',
    'IP Turbine': 'turbine',
    'LP Turbine': 'turbine',
    'Turbine Rotor': 'turbine',
    'Turbine Bearings': 'turbine',
    'Lubrication System': 'turbine',
    'Steam Valves': 'turbine',
    'Turning Gear': 'turbine',
    'Gland Seals': 'turbine',
    
    # Generator related
    'Generator': 'generator',
    'Stator': 'generator',
    'Rotor': 'generator',
    'Exciter': 'generator',
    'AVR': 'generator',
    'Generator Bearings': 'generator',
    'Hydrogen Cooling': 'generator',
    'Seal Oil System': 'generator',
    'Stator Cooling Water': 'generator',
    'Neutral Grounding': 'generator',
    
    # Condenser related
    'Condenser': 'condenser',
    'Hotwell': 'condenser',
    'Air Extraction': 'condenser',
    'Vacuum Breaking Valve': 'condenser',
    'Water Box': 'condenser',
    
    # Cooling Tower related
    'Cooling Tower': 'cooling-tower',
    'CT Fan': 'cooling-tower',
    'CT Fan Motor': 'cooling-tower',
    'CT Gearbox': 'cooling-tower',
    'CT Fan Blades': 'cooling-tower',
    
    # Coal Handling related
    'Coal Handling': 'coal-handling',
    'Wagon Tippler': 'coal-handling',
    'Stacker-Reclaimer': 'coal-handling',
    'Crusher': 'coal-handling',
    'Vibrating Screen': 'coal-handling',
    'Conveyor Belt': 'coal-handling',
    'Magnetic Separator': 'coal-handling',
    
    # Ash Handling related
    'Ash Handling': 'ash-handling',
    'Bottom Ash Hopper': 'ash-handling',
    'Clinker Grinder': 'ash-handling',
    'Ash Slurry Pump': 'ash-handling',
    'Hydro-cyclone': 'ash-handling',
    'Ash Silo': 'ash-handling',
    'Dust Suppression System': 'ash-handling',
    
    # Water Treatment related
    'Water Treatment': 'water-treatment',
    'DM Plant': 'water-treatment',
    'Clarifier': 'water-treatment',
    'Filter': 'water-treatment',
    'RO System': 'water-treatment',
    'Degasser Tower': 'water-treatment',
    'Cation Exchanger': 'water-treatment',
    'Anion Exchanger': 'water-treatment',
    'Mixed Bed Exchanger': 'water-treatment',
    
    # Electrical System related
    'Electrical System': 'electrical-system',
    'Generator Transformer': 'electrical-system',
    'Station Transformer': 'electrical-system',
    'Switchyard': 'electrical-system',
    'Switchgear': 'electrical-system',
    'Motor Control Center': 'electrical-system',
    'Battery Bank': 'electrical-system',
    'DC System': 'electrical-system',
    'Emergency Diesel Generator': 'electrical-system',
    
    # Instrumentation related
    'Instrumentation & Control': 'instrumentation-control',
    'DCS': 'instrumentation-control',
    'PLC': 'instrumentation-control',
    'Control Valve': 'instrumentation-control',
    'Actuator': 'instrumentation-control',
    'Vibration Monitoring': 'instrumentation-control',
    'Field Sensors': 'instrumentation-control',
    'Flow Meter': 'instrumentation-control',
    'Pressure Transmitter': 'instrumentation-control',
    'Temperature Transmitter': 'instrumentation-control',
    'Level Transmitter': 'instrumentation-control',
    'Analyzer': 'instrumentation-control',
}

def get_equipment_id(equipment_name):
    """Get equipment_id from mapping"""
    # Try exact match
    if equipment_name in EQUIPMENT_ID_MAPPING:
        return EQUIPMENT_ID_MAPPING[equipment_name]
    
    # Try case-insensitive match
    for key, value in EQUIPMENT_ID_MAPPING.items():
        if key.lower() == equipment_name.lower():
            return value
    
    # Try partial match (find any key containing this name)
    for key, value in EQUIPMENT_ID_MAPPING.items():
        if key.lower() in equipment_name.lower() or equipment_name.lower() in key.lower():
            return value
    
    # Try to extract base equipment name (remove parentheses content)
    base_name = re.sub(r'\s*\([^)]*\)', '', equipment_name).strip()
    if base_name in EQUIPMENT_ID_MAPPING:
        return EQUIPMENT_ID_MAPPING[base_name]
    
    # If not found, generate fallback
    return None

def get_path_from_equipment_id(equipment_id):
    """Get path from equipment_id based on your equipment tree"""
    # Define path mappings based on your equipment tree hierarchy
    path_mappings = {
        # Boiler System
        'BOILER-DRUM-001': '/PLANT-001/BOILER-001/BOILER-DRUM-001',
        'FEEDWATER-SYS-001': '/PLANT-001/BOILER-001/FEEDWATER-SYS-001',
        'FURNACE-001': '/PLANT-001/BOILER-001/FURNACE-001',
        'SUPERHEATER-001': '/PLANT-001/BOILER-001/SUPERHEATER-001',
        'ECONOMIZER-001': '/PLANT-001/BOILER-001/ECONOMIZER-001',
        'AIR-SYS-001': '/PLANT-001/BOILER-001/AIR-SYS-001',
        'ID-FAN-001': '/PLANT-001/BOILER-001/ID-FAN-001',
        'FUEL-SYS-001': '/PLANT-001/BOILER-001/FUEL-SYS-001',
        'STEAM-OUT-001': '/PLANT-001/BOILER-001/STEAM-OUT-001',
        'SAFETY-SYS-001': '/PLANT-001/BOILER-001/SAFETY-SYS-001',
        
        # Turbine System
        'HP-TURBINE-001': '/PLANT-001/TURBINE-001/HP-TURBINE-001',
        'IP-TURBINE-001': '/PLANT-001/TURBINE-001/IP-TURBINE-001',
        'LP-TURBINE-001': '/PLANT-001/TURBINE-001/LP-TURBINE-001',
        'TURBINE-ROTOR-001': '/PLANT-001/TURBINE-001/TURBINE-ROTOR-001',
        'TURBINE-BEARINGS-001': '/PLANT-001/TURBINE-001/TURBINE-BEARINGS-001',
        'LUBE-SYS-001': '/PLANT-001/TURBINE-001/LUBE-SYS-001',
        'STEAM-VALVES-001': '/PLANT-001/TURBINE-001/STEAM-VALVES-001',
        'TURNING-GEAR-001': '/PLANT-001/TURBINE-001/TURNING-GEAR-001',
        'GLAND-SEALS-001': '/PLANT-001/TURBINE-001/GLAND-SEALS-001',
        
        # Generator System
        'GENERATOR-001': '/PLANT-001/GENERATOR-001',
        'STATOR-001': '/PLANT-001/GENERATOR-001/STATOR-001',
        'ROTOR-GEN-001': '/PLANT-001/GENERATOR-001/ROTOR-GEN-001',
        'EXCITER-001': '/PLANT-001/GENERATOR-001/EXCITER-001',
        'AVR-001': '/PLANT-001/GENERATOR-001/EXCITER-001/AVR-001',
        'GEN-BEARINGS-001': '/PLANT-001/GENERATOR-001/GEN-BEARINGS-001',
        'H2-COOLING-001': '/PLANT-001/GENERATOR-001/H2-COOLING-001',
        'H2-CONTROL-001': '/PLANT-001/GENERATOR-001/H2-COOLING-001/H2-CONTROL-001',
        'SEAL-OIL-001': '/PLANT-001/GENERATOR-001/H2-COOLING-001/SEAL-OIL-001',
        'SCW-SYS-001': '/PLANT-001/GENERATOR-001/SCW-SYS-001',
        'GEN-TRANSFORMER-001': '/PLANT-001/GENERATOR-001/GEN-TRANSFORMER-001',
        'NEUTRAL-GRD-001': '/PLANT-001/GENERATOR-001/NEUTRAL-GRD-001',
        
        # Condenser System
        'CONDENSER-001': '/PLANT-001/CONDENSER-001',
        'COND-SHELL-001': '/PLANT-001/CONDENSER-001/COND-SHELL-001',
        'COND-TUBES-001': '/PLANT-001/CONDENSER-001/COND-TUBES-001',
        'WATER-BOX-IN-001': '/PLANT-001/CONDENSER-001/WATER-BOX-IN-001',
        'WATER-BOX-OUT-001': '/PLANT-001/CONDENSER-001/WATER-BOX-OUT-001',
        'TUBE-SHEETS-001': '/PLANT-001/CONDENSER-001/TUBE-SHEETS-001',
        'HOTWELL-001': '/PLANT-001/CONDENSER-001/HOTWELL-001',
        'AIR-EXTRACT-001': '/PLANT-001/CONDENSER-001/AIR-EXTRACT-001',
        'VACUUM-VALVE-001': '/PLANT-001/CONDENSER-001/VACUUM-VALVE-001',
        
        # Cooling Tower System
        'CT-001': '/PLANT-001/CT-001',
        'CT-STRUCTURE-001': '/PLANT-001/CT-001/CT-STRUCTURE-001',
        'CT-FILL-001': '/PLANT-001/CT-001/CT-FILL-001',
        'CT-DRIFT-ELIM-001': '/PLANT-001/CT-001/CT-DRIFT-ELIM-001',
        'CT-WATER-DIST-001': '/PLANT-001/CT-001/CT-WATER-DIST-001',
        'CT-BASIN-001': '/PLANT-001/CT-001/CT-BASIN-001',
        'CT-FAN-001': '/PLANT-001/CT-001/CT-FAN-001',
        'CT-FAN-MOTOR-001': '/PLANT-001/CT-001/CT-FAN-001/CT-FAN-MOTOR-001',
        'CT-GEARBOX-001': '/PLANT-001/CT-001/CT-FAN-001/CT-GEARBOX-001',
        'CT-FAN-BLADES-001': '/PLANT-001/CT-001/CT-FAN-001/CT-FAN-BLADES-001',
        'CT-MAKEUP-VALVE-001': '/PLANT-001/CT-001/CT-MAKEUP-VALVE-001',
        'CT-BLOWDOWN-001': '/PLANT-001/CT-001/CT-BLOWDOWN-001',
        'CT-LOUVERS-001': '/PLANT-001/CT-001/CT-LOUVERS-001',
        
        # Coal Handling System
        'COAL-HAND-001': '/PLANT-001/COAL-HAND-001',
        'WAGON-TIPPLER-001': '/PLANT-001/COAL-HAND-001/WAGON-TIPPLER-001',
        'STACKER-RECLAIMER-001': '/PLANT-001/COAL-HAND-001/STACKER-RECLAIMER-001',
        'CRUSHER-001': '/PLANT-001/COAL-HAND-001/CRUSHER-001',
        'VIBRATING-SCREEN-001': '/PLANT-001/COAL-HAND-001/VIBRATING-SCREEN-001',
        'CONVEYOR-001': '/PLANT-001/COAL-HAND-001/CONVEYOR-001',
        'MAG-SEP-001': '/PLANT-001/COAL-HAND-001/MAG-SEP-001',
        
        # Ash Handling System
        'ASH-HAND-001': '/PLANT-001/ASH-HAND-001',
        'BA-HOPPER-001': '/PLANT-001/ASH-HAND-001/BA-HOPPER-001',
        'CLINKER-GRINDER-001': '/PLANT-001/ASH-HAND-001/CLINKER-GRINDER-001',
        'ASH-SLURRY-PUMP-001': '/PLANT-001/ASH-HAND-001/ASH-SLURRY-PUMP-001',
        'HYDRO-CYCLONE-001': '/PLANT-001/ASH-HAND-001/HYDRO-CYCLONE-001',
        'ASH-SILO-001': '/PLANT-001/ASH-HAND-001/ASH-SILO-001',
        'DUST-SUPPRESS-001': '/PLANT-001/ASH-HAND-001/DUST-SUPPRESS-001',
        
        # Water Treatment System
        'WATER-TREAT-001': '/PLANT-001/WATER-TREAT-001',
        'CLARIFIER-001': '/PLANT-001/WATER-TREAT-001/CLARIFIER-001',
        'FILTER-001': '/PLANT-001/WATER-TREAT-001/FILTER-001',
        'DM-PLANT-001': '/PLANT-001/WATER-TREAT-001/DM-PLANT-001',
        'CATION-EX-001': '/PLANT-001/WATER-TREAT-001/DM-PLANT-001/CATION-EX-001',
        'ANION-EX-001': '/PLANT-001/WATER-TREAT-001/DM-PLANT-001/ANION-EX-001',
        'MIXED-BED-001': '/PLANT-001/WATER-TREAT-001/DM-PLANT-001/MIXED-BED-001',
        'RO-SYSTEM-001': '/PLANT-001/WATER-TREAT-001/RO-SYSTEM-001',
        'DEGASSER-001': '/PLANT-001/WATER-TREAT-001/DEGASSER-001',
        
        # Electrical System
        'ELECTRICAL-001': '/PLANT-001/ELECTRICAL-001',
        'STATION-TRANSFORMER-001': '/PLANT-001/ELECTRICAL-001/STATION-TRANSFORMER-001',
        'SWITCHYARD-001': '/PLANT-001/ELECTRICAL-001/SWITCHYARD-001',
        'SWITCHGEAR-HV-001': '/PLANT-001/ELECTRICAL-001/SWITCHGEAR-HV-001',
        'SWITCHGEAR-LV-001': '/PLANT-001/ELECTRICAL-001/SWITCHGEAR-LV-001',
        'MCC-001': '/PLANT-001/ELECTRICAL-001/MCC-001',
        'BATTERY-BANK-001': '/PLANT-001/ELECTRICAL-001/BATTERY-BANK-001',
        'BATTERY-CHARGER-001': '/PLANT-001/ELECTRICAL-001/BATTERY-BANK-001/BATTERY-CHARGER-001',
        'EMERGENCY-DG-001': '/PLANT-001/ELECTRICAL-001/EMERGENCY-DG-001',
        'GEN-TRANSFORMER-ELEC-001': '/PLANT-001/ELECTRICAL-001/GEN-TRANSFORMER-ELEC-001',
        
        # Instrumentation & Control
        'IC-001': '/PLANT-001/IC-001',
        'DCS-001': '/PLANT-001/IC-001/DCS-001',
        'PLC-001': '/PLANT-001/IC-001/PLC-001',
        'FIELD-SENSORS-001': '/PLANT-001/IC-001/FIELD-SENSORS-001',
        'PRESSURE-TRANS-001': '/PLANT-001/IC-001/FIELD-SENSORS-001/PRESSURE-TRANS-001',
        'TEMP-TRANS-001': '/PLANT-001/IC-001/FIELD-SENSORS-001/TEMP-TRANS-001',
        'FLOW-METERS-001': '/PLANT-001/IC-001/FIELD-SENSORS-001/FLOW-METERS-001',
        'LEVEL-TRANS-001': '/PLANT-001/IC-001/FIELD-SENSORS-001/LEVEL-TRANS-001',
        'ACTUATORS-001': '/PLANT-001/IC-001/ACTUATORS-001',
        'CONTROL-VALVES-001': '/PLANT-001/IC-001/CONTROL-VALVES-001',
        'VIBRATION-MON-001': '/PLANT-001/IC-001/VIBRATION-MON-001',
        'ANALYZERS-001': '/PLANT-001/IC-001/ANALYZERS-001',
        'CONTROL-ROOM-001': '/PLANT-001/IC-001/CONTROL-ROOM-001',
    }

    # Return mapped path if exists
    if equipment_id in path_mappings:
        return path_mappings[equipment_id]
    
    # Try to determine path based on equipment ID pattern
    if 'BOILER' in equipment_id or 'FEEDWATER' in equipment_id or 'FURNACE' in equipment_id or 'SUPERHEATER' in equipment_id:
        return f"/PLANT-001/BOILER-001/{equipment_id}"
    elif 'TURBINE' in equipment_id or 'LUBE' in equipment_id or 'VALVES' in equipment_id:
        return f"/PLANT-001/TURBINE-001/{equipment_id}"
    elif 'GENERATOR' in equipment_id or 'STATOR' in equipment_id or 'EXCITER' in equipment_id:
        return f"/PLANT-001/GENERATOR-001/{equipment_id}"
    elif 'CONDENSER' in equipment_id:
        return f"/PLANT-001/CONDENSER-001/{equipment_id}"
    elif 'CT-' in equipment_id or 'COOLING' in equipment_id:
        return f"/PLANT-001/CT-001/{equipment_id}"
    elif 'COAL' in equipment_id:
        return f"/PLANT-001/COAL-HAND-001/{equipment_id}"
    elif 'ASH' in equipment_id:
        return f"/PLANT-001/ASH-HAND-001/{equipment_id}"
    elif 'WATER' in equipment_id or 'DM-' in equipment_id or 'RO-' in equipment_id:
        return f"/PLANT-001/WATER-TREAT-001/{equipment_id}"
    elif 'ELECTRICAL' in equipment_id or 'SWITCH' in equipment_id or 'TRANSFORMER' in equipment_id or 'MCC' in equipment_id or 'BATTERY' in equipment_id:
        return f"/PLANT-001/ELECTRICAL-001/{equipment_id}"
    elif 'IC-' in equipment_id or 'DCS' in equipment_id or 'PLC' in equipment_id:
        return f"/PLANT-001/IC-001/{equipment_id}"
    else:
        return f"/PLANT-001/UNKNOWN/{equipment_id}"

def get_kafka_topic(equipment_name):
    """Get Kafka topic based on equipment name"""
    equipment_lower = equipment_name.lower()
    
    # Try exact match first
    for key, topic in KAFKA_TOPIC_MAPPING.items():
        if key.lower() == equipment_lower:
            return topic
    
    # Try partial match
    for key, topic in KAFKA_TOPIC_MAPPING.items():
        if key.lower() in equipment_lower:
            return topic
    
    # Default based on category
    if any(x in equipment_lower for x in ['boiler', 'feedwater', 'furnace']):
        return 'boiler_data'
    elif any(x in equipment_lower for x in ['turbine', 'generator', 'condenser']):
        return 'turbine_data'
    elif 'cooling tower' in equipment_lower:
        return 'cooling_tower_data'
    elif 'coal' in equipment_lower:
        return 'coal_handling_data'
    elif 'ash' in equipment_lower:
        return 'ash_handling_data'
    elif 'water' in equipment_lower:
        return 'water_treatment_data'
    elif any(x in equipment_lower for x in ['electrical', 'mcc', 'battery', 'dc system']):
        return 'electrical-system'
    elif any(x in equipment_lower for x in ['instrumentation', 'control', 'dcs', 'plc']):
        return 'instrumentation_data'
    else:
        return 'default_data'

def infer_data_type(tag_name, unit):
    """Infer data type from tag name and unit"""
    tag_upper = tag_name.upper()
    
    # State/Status/Flag/Command types
    state_patterns = ['STATUS', 'STATE', 'ALARM', 'TRIP', 'CMD', 'COMMAND', 
                      'FLAG', 'MODE', 'PERMISSION', 'LOCK', 'INTERLOCK',
                      'FAILURE', 'FAULT', 'HEALTH', 'ACTIVE', 'READY',
                      'RUNNING', 'STOP', 'START', 'OPEN', 'CLOSE', 'ENABLE',
                      'DETECTION', 'PROTECTION', 'SAFETY', 'SWITCH']
    
    for pattern in state_patterns:
        if pattern in tag_upper:
            return 'BOOLEAN'
    
    # Count types
    if any(x in tag_upper for x in ['COUNT', 'COUNTER', 'CYCLES', 'STARTS']):
        return 'INTEGER'
    
    # String types
    if any(x in tag_upper for x in ['REASON', 'MESSAGE', 'DESCRIPTION', 'VERSION']):
        return 'STRING'
    
    # Default to FLOAT for most process values
    return 'FLOAT'

def determine_scan_rate(tag_name):
    """Determine scan rate based on tag name"""
    tag_upper = tag_name.upper()
    
    if any(x in tag_upper for x in ['VIBRATION', 'SPEED', 'RPM']):
        return 500  # High frequency data
    elif any(x in tag_upper for x in ['ALARM', 'TRIP', 'STATUS', 'STATE', 'CMD', 'SWITCH']):
        return 5000  # Status changes
    elif any(x in tag_upper for x in ['TEMP', 'PRESSURE', 'FLOW']):
        return 1000  # Normal process data
    elif 'LEVEL' in tag_upper:
        return 1000  # Level data
    else:
        return 2000  # Default

def parse_sql_insert_line(line):
    """Parse a single SQL INSERT line and extract values"""
    # Pattern to match: (1001,'Equipment','TAG_NAME','unit','description'),
    pattern = r"\((\d+),'([^']*)','([^']*)','([^']*)','([^']*)'\)[,;]"
    match = re.search(pattern, line)
    
    if match:
        return {
            'pi_point_id': int(match.group(1)),
            'equipment': match.group(2),
            'tag_name': match.group(3),
            'unit': match.group(4),
            'description': match.group(5)
        }
    return None

def format_value_for_sql(value):
    """Format a value for SQL insertion, handling special characters"""
    if isinstance(value, str):
        # Escape single quotes
        value = value.replace("'", "''")
        return f"'{value}'"
    return str(value)

def main():
    """Main function to process the input file and generate a single INSERT statement"""
    
    input_path = Path(INPUT_FILE)
    output_path = Path(OUTPUT_FILE)
    
    if not input_path.exists():
        print(f"Error: Input file '{INPUT_FILE}' not found!")
        sys.exit(1)
    
    print(f"Processing {INPUT_FILE}...")
    
    # Read input file
    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split into lines and process
    lines = content.split('\n')
    
    records = []
    skipped_records = 0
    
    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if line.startswith('(') and not line.startswith('(-'):
            record = parse_sql_insert_line(line)
            if record:
                records.append(record)
            else:
                skipped_records += 1
                print(f"Warning: Could not parse line {line_num}")
    
    print(f"Found {len(records)} valid records to transform")
    if skipped_records > 0:
        print(f"Skipped {skipped_records} unparseable lines")
    
    if not records:
        print("No records found to transform!")
        return
    
    # Generate timestamp for created_at
    created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Build the single INSERT statement
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("-- Generated tag_metadata INSERT statement\n")
        f.write(f"-- Source: {INPUT_FILE}\n")
        f.write(f"-- Generated: {created_at}\n")
        f.write(f"-- Total records: {len(records)}\n")
        f.write("-- NOTE: This is a LOOKUP table - NO partitioning needed!\n")
        f.write("-- Partitioning is for tag_timeseries table only.\n\n")
        
        f.write("INSERT INTO tag_metadata (pi_point_id, tag_name, equipment_id, path, unit, description, data_type, scan_rate, kafka_topic, enabled, created_at) VALUES\n")
        
        # Process each record
        values_list = []
        equipment_not_found = set()
        
        for i, record in enumerate(records):
            try:
                # Get equipment_id from mapping
                equipment_id = get_equipment_id(record['equipment'])
                
                if not equipment_id:
                    equipment_not_found.add(record['equipment'])
                    # Generate fallback equipment_id
                    equipment_id = record['equipment'].upper().replace(' ', '-').replace('(', '').replace(')', '').replace('/', '-')[:50]
                
                # Get path based on equipment_id
                path = get_path_from_equipment_id(equipment_id)
                
                # Get Kafka topic
                kafka_topic = get_kafka_topic(record['equipment'])
                
                # Infer data type
                data_type = infer_data_type(record['tag_name'], record['unit'])
                
                # Determine scan rate
                scan_rate = determine_scan_rate(record['tag_name'])
                
                # Format all values (NO partition_num column!)
                values = [
                    str(record['pi_point_id']),
                    format_value_for_sql(record['tag_name']),
                    format_value_for_sql(equipment_id),
                    format_value_for_sql(path),
                    format_value_for_sql(record['unit']),
                    format_value_for_sql(record['description']),
                    format_value_for_sql(data_type),
                    str(scan_rate),
                    format_value_for_sql(kafka_topic),
                    str(DEFAULT_ENABLED),
                    format_value_for_sql(created_at)
                ]
                
                # Join values with commas
                value_str = f"({', '.join(values)})"
                
                # Add comma for all but the last record
                if i < len(records) - 1:
                    value_str += ","
                
                values_list.append(value_str)
                
            except Exception as e:
                print(f"Error processing record {i+1}: {record}")
                print(f"Error: {e}")
                continue
        
        # Write all values
        f.write('\n'.join(values_list))
        f.write(';\n')
        
        # Add footer with statistics
        f.write(f"\n-- Successfully inserted {len(values_list)} records\n")
        
        if equipment_not_found:
            f.write(f"\n-- Equipment not found in mapping ({len(equipment_not_found)}):\n")
            for eq in sorted(equipment_not_found)[:50]:
                f.write(f"--   {eq}\n")
    
    print(f"Successfully transformed {len(values_list)} records")
    print(f"Output written to: {OUTPUT_FILE}")
    
    # Print summary statistics
    data_types = {}
    equipment_counts = {}
    
    for record in records:
        dt = infer_data_type(record['tag_name'], record['unit'])
        data_types[dt] = data_types.get(dt, 0) + 1
        
        # Count by equipment
        equipment_counts[record['equipment']] = equipment_counts.get(record['equipment'], 0) + 1
    
    print("\nSummary Statistics:")
    print(f"Total records: {len(records)}")
    print(f"\nData Types:")
    for dt, count in sorted(data_types.items(), key=lambda x: x[1], reverse=True):
        print(f"  {dt}: {count} ({count/len(records)*100:.1f}%)")
    
    if equipment_not_found:
        print(f"\n⚠️  Equipment not found in mapping ({len(equipment_not_found)}):")
        for eq in sorted(equipment_not_found)[:20]:
            print(f"  - {eq}")

if __name__ == "__main__":
    main()