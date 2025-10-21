#!/usr/bin/env python3

import json
import sys
import os
from typing import List, Dict, Any
from K8sCurlExecutor import K8sCurlExecutor

class BaseInventoryHandler:
    """Base class for inventory handlers with common functionality"""
    
    def __init__(self, config_file: str = None):
        """Initialize the Inventory Handler with configuration"""
        # Use default config.json from current directory if not provided
        if config_file is None:
            config_file = os.path.join(os.getcwd(), 'inventory_config.json')
        
        self.config_file = config_file
        self.config = self._load_config(config_file)
        self.executor = K8sCurlExecutor(self.config['k8s_config'])
        
        # Set up secret configs and token config from the main config
        self.executor.secret_configs = self.config.get('secret_configs', {})
        self.executor.token_config = self.config.get('token_config', {})
        self.executor.commands = self.config.get('commands', {})
        
        # Results storage
        self.results = {}
        self.failed_inventory = []
        
        # Register hooks (to be implemented by subclasses)
        self._register_hooks()
    
    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            print(f"📄 Loading configuration from: {config_file}")
            with open(config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: Configuration file '{config_file}' not found")
            print(f"Expected location: {os.path.abspath(config_file)}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in configuration file: {e}")
            sys.exit(1)
    
    def _register_hooks(self):
        """Register pre and post hooks for the executor - to be implemented by subclasses"""
        pass
    
    def save_results(self, output_file: str):
        """Save results to a JSON file"""
        try:
            with open(output_file, 'w') as f:
                json.dump(self.results, f, indent=2)
            print(f"💾 Results saved to: {output_file}")
        except Exception as e:
            print(f"❌ Failed to save results: {e}")
    
    def print_summary(self):
        """Print a summary of the results"""
        total = len(self.results)
        successful = sum(1 for r in self.results.values() if r['success'])
        failed = total - successful
        
        print(f"\n📊 Summary:")
        print(f"   Total inventory processed: {total}")
        print(f"   ✅ Successful: {successful}")
        print(f"   ❌ Failed: {failed}")
        
        if self.failed_inventory:
            print(f"\n❌ Failed Inventory:")
            for inv_id in self.failed_inventory:
                error = self.results[inv_id].get('error', 'Unknown error')
                print(f"   - {inv_id}: {error}")
    
    def load_ids_from_file(self, file_path: str) -> List[str]:
        """Load order IDs from a file (one per line)"""
        try:
            with open(file_path, 'r') as f:
                order_ids = [line.strip() for line in f if line.strip()]
            return order_ids
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error reading file '{file_path}': {e}")
            sys.exit(1)
