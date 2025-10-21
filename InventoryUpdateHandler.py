
#!/usr/bin/env python3

import argparse
import json
import sys
import os
from typing import List, Dict, Any
from BaseInventoryHandler import BaseInventoryHandler


class InventoryUpdateHandler(BaseInventoryHandler):

    def _register_hooks(self):
        """Register pre and post hooks for the executor"""
        
        def pre_get_inventory_hook(context: Dict[str, Any]) -> bool:
            """Pre-hook for getting inventory"""
            command_id = context['command_id']
            inv_id = context.get('inv_id')
            executor = context.get('executor')
            
            print(f"🔍 Starting to fetch Inventory: {inv_id}")
            if executor:
                executor.logger.info(f"Pre-hook: Preparing to fetch Inventory: {inv_id}")
            
            return True  # Continue execution
        
        def post_get_inventory_hook(context: Dict[str, Any]) -> bool:
            """Post-hook for getting inventory"""
            command_id = context['command_id']
            inv_id = context.get('inv_id')
            success = context.get('success', False)
            response = context.get('response')
            http_status = context.get('http_status', 0)
            response_headers = context.get('response_headers', {})
            trace_id = context.get('trace_id')
            error = context.get('error')
            executor = context.get('executor')
            
            if executor:
                executor.logger.info(f"Post-hook for Get Inventory: {inv_id}, success={success}, http_status={http_status}, trace_id={trace_id}")
            
            if success and response:
                try:
                    # Store the successful result
                    self.results[inv_id] = {
                        'success': True,
                        'data': response,
                        'http_status': http_status,
                        'trace_id': trace_id,
                        'response_headers': response_headers
                    }
                    
                    print(f"✅ Successfully fetched Inventory: {inv_id}")
                    if executor:
                        executor.logger.info(f"Post-hook: Successfully processed Inventory: {inv_id}")
                    
                    # Log some key information about the Inventory
                    if isinstance(response, dict):
                        id = response.get('id', 'Unknown')
                        status = response.get('status', 'Unknown')
                        print(f"   📋 ID: {id}, Status: {status}")
                        executor.logger.debug(f"   🔍 Available fields: {list(response.keys())}")
                        
                        if trace_id:
                            print(f"   🔗 Trace ID: {trace_id}")
                        
                except Exception as e:
                    print(f"❌ Error processing successful response for Inventory: {inv_id}: {e}")
                    self.failed_invetories.append(inv_id)
                    self.results[inv_id] = {
                        'success': False,
                        'error': f'Response processing error: {e}',
                        'http_status': http_status,
                        'trace_id': trace_id,
                        'response_headers': response_headers
                    }
                    
            else:
                # Handle failed requests
                print(f"❌ Failed to fetch Inventory: {inv_id}")
                
                # Extract error details
                error_details = {
                    'success': False,
                    'error': error or 'Unknown error',
                    'http_status': http_status,
                    'trace_id': trace_id,
                    'response_headers': response_headers
                }
                
                # If we have a response body (even for errors), include it
                if response:
                    error_details['error_response'] = response
                    
                    # Try to extract more meaningful error information
                    if isinstance(response, dict):
                        reason = response.get('reason', 'Unknown')
                        message = response.get('message', response.get('detail', ''))
                        error_details['error_reason'] = reason
                        if message:
                            error_details['error_message'] = message
                
                self.failed_invetories.append(inv_id)
                self.results[inv_id] = error_details
                
                # Enhanced error logging
                if http_status:
                    print(f"   🚫 HTTP Status: {http_status}")
                if trace_id:
                    print(f"   🔗 Trace ID: {trace_id}")
                if isinstance(response, dict) and 'reason' in response:
                    print(f"   💬 Error Reason: {response['reason']}")
                
                if executor:
                    executor.logger.error(f"Failed to fetch Inventory {inv_id}: HTTP {http_status}, Trace: {trace_id}, Error: {error}")
                
            return True

        def pre_update_inventory_hook(context: Dict[str, Any]) -> bool:
            """Pre-hook for updating inventory"""
            command_id = context['command_id']
            inv_id = context.get('inv_id')
            target_status = context.get('target_status')
            executor = context.get('executor')
            
            print(f"🔄 Starting to update Inventory status: {inv_id} -> {target_status}")
            if executor:
                executor.logger.info(f"Pre-hook: Preparing to update Inventory {inv_id} to {target_status}")
            
            return True

        def post_update_inventory_hook(context: Dict[str, Any]) -> bool:
            """Post-hook for updating inventory"""
            command_id = context['command_id']
            inv_id = context.get('inv_id')
            target_status = context.get('target_status')
            success = context.get('success', False)
            response = context.get('response')
            http_status = context.get('http_status', 0)
            trace_id = context.get('trace_id')
            error = context.get('error')
            executor = context.get('executor')
            
            if executor:
                executor.logger.info(f"Post-hook for Inventory {inv_id} status update: success={success}, http_status={http_status}, trace_id={trace_id}")
            
            # Store the update result for the main logic to access
            update_key = f"{inv_id}_update_{target_status}"
            self.update_results_cache = getattr(self, 'update_results_cache', {})
            self.update_results_cache[update_key] = {
                'success': success,
                'http_status': http_status,
                'trace_id': trace_id,
                'error': error,
                'response': response
            }
            
            if success:
                print(f"✅ Successfully updated Inventory status: {inv_id} -> {target_status}")
                if trace_id:
                    print(f"   🔗 Trace ID: {trace_id}")
            else:
                print(f"❌ Failed to update Inventory status: {inv_id} -> {target_status}")
                if http_status:
                    print(f"   🚫 HTTP Status: {http_status}")
                if trace_id:
                    print(f"   🔗 Trace ID: {trace_id}")
                if error:
                    print(f"   💬 Error: {error}")
                
                if executor:
                    executor.logger.error(f"Failed to update Inventory {inv_id} status: HTTP {http_status}, Trace: {trace_id}, Error: {error}")
            
            return True
        
        # Register the hooks
        self.executor.register_pre_hook('pre_get_inventory', pre_get_inventory_hook)
        self.executor.register_post_hook('post_get_inventory', post_get_inventory_hook)
        self.executor.register_pre_hook('pre_update_inventory_status', pre_update_inventory_hook)
        self.executor.register_post_hook('post_update_inventory_status', post_update_inventory_hook)
    
    def _get_curl_config(self, inv_id: str) -> Dict[str, Any]:
        """Get curl configuration for fetching a specific inventory"""
        base_config = self.config['commands']['get_inv_by_id'].copy()
        
        # Substitute the Inventory ID in the URL
        base_config['url'] = base_config['url'].replace('{inv_id}', inv_id)
        
        # Add Inventory ID to context for hooks
        base_config['inv_id'] = inv_id

        # Add pre and post hooks
        base_config['pre_hooks'] = ['pre_get_inventory']
        base_config['post_hooks'] = ['post_get_inventory']
        
        return base_config

    def _get_curl_config_update_inventory_status(self, inv_id: str, payload: Dict[str, Any], target_status: str) -> Dict[str, Any]:
        """Get curl configuration for updating inventory status"""
        base_config = self.config['commands']['update_inventory_status'].copy()
        
        # Set the payload
        base_config['data'] = json.dumps(payload)
        
        # Add context for hooks
        base_config['inv_id'] = inv_id
        base_config['target_status'] = target_status
        
        # Add pre and post hooks
        base_config['pre_hooks'] = ['pre_update_inventory_status']
        base_config['post_hooks'] = ['post_update_inventory_status']
        
        # Mark that this command requires initiator authorization
        base_config['requires_initiator_auth'] = True

        return base_config

    def _create_active_inventory_payload(self, inv_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create payload for updating inventory from terminated to active"""
        # Create a copy of the original data
        updated_payload = inv_data.copy()
        
        # Update the status from 'terminated' to 'active'
        updated_payload['status'] = 'active'
        
        print(f"🔄 Created payload to update status: terminated -> active")
        
        return updated_payload

    def fetch_inventory(self, input_inv_ids: List[str]) -> Dict[str, Any]:
        """Fetch multiple inventories"""
        print(f"🚀 Starting to fetch {len(input_inv_ids)} inventories...")
        
        # Get the first running pod
        pod = self.executor.get_first_running_pod()
        if not pod:
            print("❌ No running pods found")
            return {}
        
        print(f"📦 Using pod: {pod.metadata.name}")
        
        # Get auth token once for all requests
        print("🔐 Obtaining authentication token...")
        auth_token = self.executor.get_auth_token_for_command(pod, 'get_inv_by_id')
        
        if not auth_token:
            print("❌ Failed to obtain authentication token")
            return {}
        
        print("✅ Authentication token obtained successfully")
        
        # Fetch each inventory
        for i, inv_id in enumerate(input_inv_ids, 1):
            print(f"\n📋 Processing {i}/{len(input_inv_ids)}: {inv_id}")
            
            # Get curl configuration for this Inventory
            curl_config = self._get_curl_config(inv_id)
            
            # Execute the curl command (hooks are already configured in curl_config)
            result = self.executor.execute_curl_command(pod, curl_config, auth_token)
        
        return self.results

    def update_terminated_to_active(self, input_inv_ids: List[str]) -> Dict[str, Any]:
        """Update inventories from terminated to active status"""
        print(f"🔄 Starting to update {len(input_inv_ids)} inventories from terminated to active...")
        
        # Get the first running pod
        pod = self.executor.get_first_running_pod()
        if not pod:
            print("❌ No running pods found")
            return {}
        
        print(f"📦 Using pod: {pod.metadata.name}")
        
        # Get auth token once for all requests
        print("🔐 Obtaining authentication token...")
        auth_token_get_inventory = self.executor.get_auth_token_for_command(pod, 'get_inv_by_id')
        auth_token_update_inventory = self.executor.get_auth_token_for_command(pod, 'update_inventory_status')
        
        if not auth_token_update_inventory or not auth_token_get_inventory:
            print("❌ Failed to obtain authentication token")
            return {}
        
        print("✅ Authentication token obtained successfully")
        
        update_results = {}
        terminated_inventories = []  # Collect all terminated inventories for batch update
        
        for i, inv_id in enumerate(input_inv_ids, 1):
            print(f"\n🔄 Processing {i}/{len(input_inv_ids)}: {inv_id}")
            update_results[inv_id] = {'steps': []}
            
            try:
                # Step 1: Fetch current inventory
                print(f"📋 Step 1: Fetching current status of Inventory: {inv_id}")
                curl_config = self._get_curl_config(inv_id)
                result = self.executor.execute_curl_command(pod, curl_config, auth_token_get_inventory)
                
                if inv_id not in self.results or not self.results[inv_id].get('success'):
                    print(f"❌ Failed to fetch Inventory {inv_id}, skipping update")
                    update_results[inv_id]['error'] = 'Failed to fetch initial Inventory status'
                    update_results[inv_id]['overall_success'] = False
                    continue
                
                inv_data = self.results[inv_id]['data']
                current_status = inv_data.get('status', 'Unknown')
                print(f"📊 Current Inventory status: {current_status}")
                update_results[inv_id]['steps'].append({'step': 'fetch_initial', 'success': True, 'current_status': current_status})
                
                # Step 2: Check if status is 'terminated'
                if current_status.lower() != 'terminated':
                    print(f"ℹ️  Inventory {inv_id} is not in terminated status (current: {current_status}), skipping")
                    update_results[inv_id]['steps'].append({'step': 'status_check', 'success': True, 'message': f'Not terminated (status: {current_status})'})
                    update_results[inv_id]['overall_success'] = True
                    continue
                
                print(f"✅ Inventory {inv_id} is in terminated status, preparing for update")
                update_results[inv_id]['steps'].append({'step': 'status_check', 'success': True, 'message': 'Status is terminated'})
                
                # Step 3: Create payload with status changed to 'active'
                active_payload = self._create_active_inventory_payload(inv_data)
                terminated_inventories.append({
                    'inv_id': inv_id,
                    'payload': active_payload
                })
                
                print(f"📦 Added Inventory {inv_id} to batch update list")
                update_results[inv_id]['steps'].append({'step': 'prepare_payload', 'success': True})
                
            except Exception as e:
                print(f"❌ Error processing Inventory {inv_id}: {e}")
                update_results[inv_id]['error'] = str(e)
                update_results[inv_id]['overall_success'] = False
        
        # Step 4: Batch update all terminated inventories
        if terminated_inventories:
            print(f"\n🔄 Batch updating {len(terminated_inventories)} terminated inventories to active...")
            
            for inventory in terminated_inventories:
                inv_id = inventory['inv_id']
                payload = inventory['payload']
                
                try:
                    print(f"🔄 Updating Inventory: {inv_id} from terminated -> active")
                    
                    # Get curl configuration for update
                    update_config = self._get_curl_config_update_inventory_status(inv_id, payload, 'active')
                    
                    # Execute the update command
                    update_result = self.executor.execute_curl_command(pod, update_config, auth_token_update_inventory)
                    
                    # Check the result from the hook
                    update_key = f"{inv_id}_update_active"
                    hook_result = getattr(self, 'update_results_cache', {}).get(update_key, {})

                    if hook_result.get('success', False):
                        print(f"✅ Successfully updated Inventory {inv_id} to active")
                        update_results[inv_id]['steps'].append({'step': 'update_to_active', 'success': True})
                        update_results[inv_id]['overall_success'] = True
                    else:
                        print(f"❌ Failed to update Inventory {inv_id} to active")
                        error_msg = hook_result.get('error', 'AInventory call failed')
                        update_results[inv_id]['steps'].append({'step': 'update_to_active', 'success': False, 'error': error_msg})
                        update_results[inv_id]['overall_success'] = False
                
                except Exception as e:
                    print(f"❌ Error updating Inventory {inv_id}: {e}")
                    update_results[inv_id]['steps'].append({'step': 'update_to_active', 'success': False, 'error': str(e)})
                    update_results[inv_id]['overall_success'] = False
        else:
            print(f"ℹ️  No terminated inventory found to update")
        
        return update_results


def main():
    parser = argparse.ArgumentParser(description='Fetch and Update Inventory from Terminated to Active using K8sCurlExecutor')
    parser.add_argument('--config', '-c', 
                       help='Configuration file path (JSON). Default: ./config.json')
    parser.add_argument('--inv-ids', '-p', nargs='+', 
                       help='Inventory IDs (space-separated)')
    parser.add_argument('--inv-file', '-f',
                       help='File containing Inventory IDs (one per line)')
    parser.add_argument('--output', '-o', 
                       help='Output file for results (JSON)')
    parser.add_argument('--action', '-a', choices=['fetch', 'update'], default='fetch',
                       help='Action to perform: fetch (default) or update (terminated to active)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.inv_ids and not args.inv_file:
        print("Error: Either --inv-ids or --inv-file must be specified")
        sys.exit(1)
    
    if args.inv_ids and args.inv_file:
        print("Error: Cannot specify both --inv-ids and --inv-file")
        sys.exit(1)
    
    # Initialize handler
    handler = InventoryUpdateHandler(args.config)
    
    # Load inventory IDs
    if args.inv_file:
        input_inv_ids = handler.load_ids_from_file(args.inv_file)
        print(f"📁 Loaded {len(input_inv_ids)} Inventory IDs from file: {args.inv_file}")
    else:
        input_inv_ids = args.inv_ids
        print(f"📝 Using {len(input_inv_ids)} Inventory IDs from command line")
    
    if not input_inv_ids:
        print("Error: No Inventory IDs provided")
        sys.exit(1)
    
    try:
        if args.action == 'fetch':
            # Fetch inventories
            results = handler.fetch_inventory(input_inv_ids)
            
            # Print summary
            handler.print_summary()
            
            # Save results if output file specified
            if args.output:
                handler.save_results(args.output)
    
        elif args.action == 'update':
            # Update terminated inventories to active
            update_results = handler.update_terminated_to_active(input_inv_ids)
            
            # Print update summary
            print(f"\n📊 Update Summary:")
            print(f"{'='*50}")
            
            successful_updates = 0
            failed_updates = 0
            skipped_updates = 0
            
            for inv_id, result in update_results.items():
                if result.get('overall_success'):
                    # Check if it was actually updated or just skipped
                    update_step = next((step for step in result.get('steps', []) if step.get('step') == 'update_to_active'), None)
                    if update_step:
                        successful_updates += 1
                        print(f"✅ {inv_id}: Successfully updated terminated -> active")
                    else:
                        skipped_updates += 1
                        print(f"⏭️  {inv_id}: Skipped (not terminated)")
                else:
                    failed_updates += 1
                    error_msg = result.get('error', 'Unknown error')
                    print(f"❌ {inv_id}: Failed - {error_msg}")
            
            print(f"\n📈 Total: {len(update_results)} | ✅ Updated: {successful_updates} | ⏭️  Skipped: {skipped_updates} | ❌ Failed: {failed_updates}")
            
            # Save update results if output file specified
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(update_results, f, indent=2)
                print(f"💾 Update results saved to: {args.output}")
    
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
