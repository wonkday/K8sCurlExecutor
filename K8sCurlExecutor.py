#!/usr/bin/env python3

import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable
import base64
import yaml
from kubernetes import client, config, stream
from kubernetes.client.rest import ApiException
import json
import re

class K8sCurlExecutor:
    def __init__(self, config_dict: Dict[str, Any]):
        """
        Initialize the Kubernetes Curl Executor using Kubernetes Python client
        
        Args:
            config_dict: Configuration dictionary
            flows_config: Flow/activity configuration dictionary
        """
        # Validate required configuration parameters# Suppress kubectl warnings globally        
        os.environ['KUBECTL_WARNINGS'] = 'off'

        # Setup logging
        self._setup_logging()

        self.microservice_name = config_dict.get('microservice_name')
        self.namespace = config_dict.get('namespace', 'default')
        self.label_selector = config_dict.get('label_selector', f'app={self.microservice_name}')
        self.container_blacklist = config_dict.get('container_blacklist', [
            'istio-proxy', 
            'istio-init', 
            'msnext-extensions'
        ])  # Default blacklist for common sidecar containers

        # Optional configuration parameters
        self.kubeconfig_path = config_dict.get('kubeconfig_path')
        self.context = config_dict.get('context')
                
        # Response storage for cross-command data sharing
        self.response_store = {}
        self.global_variables = {}
        
        # Hook functions storage
        self.pre_hooks = {}
        self.post_hooks = {}
        
        # Credentials cache with expiration time
        self.cached_credentials = {}
        self.cached_secrets = {}
              
        # Initialize Kubernetes client
        self._init_kubernetes_client()
        

    def _setup_logging(self):
        """Setup logging configuration"""
        log_dir = './logs'
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        log_file = os.path.join(log_dir, f'k8s_curl_executor_{timestamp}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s:%(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _get_secret_credentials(self, secret_name: str, username_key: str = 'username', password_key: str = 'password') -> Optional[Dict[str, str]]:
        """
        Retrieve username and password from Kubernetes secret
        
        Args:
            secret_name: Name of the Kubernetes secret
            username_key: Key name for username in the secret (default: 'username')
            password_key: Key name for password in the secret (default: 'password')
            
        Returns:
            Dictionary with username and password if successful, None otherwise
        """
        # Check cache first
        cache_key = f"{secret_name}:{username_key}:{password_key}"
        if cache_key in self.cached_credentials:
            self.logger.debug(f"Using cached credentials for secret: {secret_name}")
            return self.cached_credentials[cache_key]
        
        try:
            self.logger.info(f"Retrieving credentials from secret: {secret_name}")
            
            # Read the secret
            secret = self.v1.read_namespaced_secret(
                name=secret_name,
                namespace=self.namespace
            )
            
            if not secret.data:
                self.logger.error(f"Secret '{secret_name}' has no data")
                return None
            
            self.logger.debug(f"Secret keys available: {list(secret.data.keys())}")

            # For secrets with YAML structure, we need to find the key containing the YAML
            # Usually it's the first key or a key ending with .yaml
            yaml_key = None
            
            # Look for a key ending with .yaml first
            for key in secret.data.keys():
                if key.endswith('.yaml') or key.endswith('.yml'):
                    yaml_key = key
                    break
            
            # If no .yaml key found, try the first key
            if not yaml_key and secret.data:
                yaml_key = list(secret.data.keys())[0]
                self.logger.info(f"No .yaml key found, using first available key: {yaml_key}")
            
            if not yaml_key:
                self.logger.error(f"No suitable key found in secret '{secret_name}'")
                return None
            
            self.logger.info(f"Using secret key: {yaml_key}")
            
            # Decode the base64 encoded YAML content
            try:
                yaml_content = base64.b64decode(secret.data[yaml_key]).decode('utf-8')
                self.logger.debug(f"Decoded YAML content length: {len(yaml_content)} characters")
                self.logger.debug(f"YAML content preview: {yaml_content[:200]}...")
            except Exception as e:
                self.logger.error(f"Failed to decode base64 content from key '{yaml_key}': {e}")
                return None
            
            # Parse the YAML content
            try:
                yaml_data = yaml.safe_load(yaml_content)
                self.logger.debug(f"Successfully parsed YAML structure")
                self.logger.debug(f"YAML root keys: {list(yaml_data.keys()) if isinstance(yaml_data, dict) else 'Not a dict'}")
            except yaml.YAMLError as e:
                self.logger.error(f"Failed to parse YAML content: {e}")
                self.logger.error(f"YAML content: {yaml_content}")
                return None
            
            # Navigate through the expected structure:
            # com.wonkday.ms.security.user and com.wonkday.ms.security.password.plain
            try:
                # Navigate to com -> wonkday -> ms -> security
                security_config = yaml_data.get('com', {}).get('wonkday', {}).get('ms', {}).get('security', {})
                
                if not security_config:
                    self.logger.error("Could not find 'com.wonkday.ms.security' path in YAML structure")
                    self.logger.error(f"Available structure: {yaml_data}")
                    return None
                
                self.logger.debug(f"Security config keys: {list(security_config.keys())}")
                
                # Extract username and password
                username = security_config.get('user')
                password = security_config.get('password.plain')
                
                if not username:
                    self.logger.error("Username not found at 'com.wonkday.ms.security.user'")
                    self.logger.error(f"Available keys in security: {list(security_config.keys())}")
                    return None
                
                if not password:
                    self.logger.error("Password not found at 'com.wonkday.ms.security.password.plain'")
                    self.logger.error(f"Available keys in security: {list(security_config.keys())}")
                    return None
                
                credentials = {
                    'username': username,
                    'password': password
                }
                
                # Cache the credentials
                self.cached_credentials[cache_key] = credentials
                
                self.logger.info(f"Successfully retrieved credentials from secret: {secret_name}")
                self.logger.debug(f"Username: {username}")
                # Don't log the actual password for security
                self.logger.debug(f"Password: {'*' * len(password)}")
                
                return credentials
                
            except Exception as e:
                self.logger.error(f"Failed to extract credentials from YAML structure: {e}")
                self.logger.error(f"YAML structure: {yaml_data}")
                return None
                
        except ApiException as e:
            self.logger.error(f"Failed to read secret '{secret_name}': {e}")
            if e.status == 404:
                self.logger.error(f"Secret '{secret_name}' not found in namespace '{self.namespace}'")
            elif e.status == 403:
                self.logger.error(f"Access denied to secret '{secret_name}' in namespace '{self.namespace}'")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error reading secret '{secret_name}': {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def _init_kubernetes_client(self):
        """Initialize the Kubernetes client"""
        try:
            if self.kubeconfig_path:
                config.load_kube_config(config_file=self.kubeconfig_path, context=self.context)
            else:
                config.load_incluster_config()
            self.v1 = client.CoreV1Api()
            self.logger.info("Kubernetes client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kubernetes client: {e}")
            raise
    
    def register_pre_hook(self, name: str, func: Callable):
        """Register a pre-execution hook function"""
        self.pre_hooks[name] = func
        
    def register_post_hook(self, name: str, func: Callable):
        """Register a post-execution hook function"""
        self.post_hooks[name] = func

    def _execute_hooks(self, hook_type: str, hook_names: List[str], context: Dict[str, Any]) -> bool:
        """
        Execute pre or post hooks
        
        Args:
            hook_type: 'pre' or 'post'
            hook_names: List of hook names to execute
            context: Context dictionary to pass to hooks
            
        Returns:
            True if all hooks executed successfully, False otherwise
        """
        if not hook_names:
            return True
        
        hook_registry = self.pre_hooks if hook_type == 'pre' else self.post_hooks
        
        for hook_name in hook_names:
            if hook_name not in hook_registry:
                self.logger.warning(f"{hook_type.capitalize()}-hook '{hook_name}' not found in registry")
                continue
            
            try:
                self.logger.debug(f"Executing {hook_type}-hook: {hook_name}")
                hook_func = hook_registry[hook_name]
                
                # Execute the hook function
                result = hook_func(context)
                
                if result is False:
                    self.logger.error(f"{hook_type.capitalize()}-hook '{hook_name}' returned False")
                    return False
                    
            except Exception as e:
                self.logger.error(f"Error executing {hook_type}-hook '{hook_name}': {e}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                return False
        
        return True

    def _get_main_container_name(self, pod_name: str) -> Optional[str]:
        """
        Get the main application container name from the pod, excluding blacklisted containers
        
        Args:
            pod_name: Name of the pod
            
        Returns:
            Container name if found, None otherwise
        """
        try:
            # Get pod details
            pod = self.v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            
            # Get all container names
            all_containers = [container.name for container in pod.spec.containers]
            self.logger.debug(f"All containers in pod {pod_name}: {all_containers}")
            self.logger.debug(f"Container blacklist: {self.container_blacklist}")
            
            # Filter out blacklisted containers
            available_containers = []
            for container_name in all_containers:
                is_blacklisted = False
                
                # Check exact matches and pattern matches
                for blacklisted in self.container_blacklist:
                    if blacklisted in container_name or container_name == blacklisted:
                        is_blacklisted = True
                        self.logger.debug(f"Container '{container_name}' blacklisted (matches '{blacklisted}')")
                        break
                
                if not is_blacklisted:
                    available_containers.append(container_name)
            
            self.logger.info(f"Available containers after blacklist filtering: {available_containers}")
            
            if not available_containers:
                self.logger.error(f"No containers available after applying blacklist for pod {pod_name}")
                return None
            
            # Priority order for container selection from remaining containers
            preferred_patterns = [
                'application-container',
                'service-container',
                f'{self.microservice_name}',
                'app-container',
                'main-container'
            ]
            
            # Try to find preferred containers first
            for preferred in preferred_patterns:
                for container_name in available_containers:
                    if preferred in container_name:
                        self.logger.info(f"Selected preferred container: {container_name}")
                        return container_name
            
            # If no preferred container found, use the first available one
            selected_container = available_containers[0]
            self.logger.info(f"Selected first available container: {selected_container}")
            return selected_container
            
        except ApiException as e:
            self.logger.error(f"Failed to get pod details: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error getting container name: {e}")
            return None

    def get_first_running_pod(self) -> Optional[client.V1Pod]:
        """
        Get the first running pod for the configured microservice
        
        Returns:
            V1Pod object if found, None otherwise
        """
        try:
            self.logger.info(f"Looking for running pods in namespace: \"{self.namespace}\" with label selector: {self.label_selector}")
            
            # List pods with label selector and field selector
            pods = self.v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=self.label_selector,
                field_selector="status.phase=Running"
            )
            
            if not pods.items:
                self.logger.error(f"No running pods found for microservice: {self.microservice_name}")
                return None
            
            # Get the first running pod
            pod = pods.items[0]
            self.logger.info(f"Found running pod: {pod.metadata.name}")
            return pod
            
        except ApiException as e:
            self.logger.error(f"Kubernetes API error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error getting pod: {e}")
            return None


    def execute_command_in_pod(self, pod_name: str, command: List[str], container_name: str = None) -> Optional[str]:
        """
        Execute a command in the specified pod
        
        Args:
            pod_name: Name of the pod
            command: Command to execute as a list
            container_name: Name of the container (optional, will auto-detect if not provided)
            
        Returns:
            Command output if successful, None otherwise
        """
        try:
            # If no container name provided, try to determine the main application container
            if not container_name:
                container_name = self._get_main_container_name(pod_name)
                if not container_name:
                    self.logger.error(f"Could not determine main container for pod {pod_name}")
                    return None
            
            #self.logger.debug(f"Executing command in pod {pod_name}, container {container_name}: {' '.join(command)}")
            self.logger.info(f"Executing command in pod {pod_name}, container {container_name}: {' '.join(command)}")
            
            # Execute command in pod
            resp = stream.stream(
                self.v1.connect_get_namespaced_pod_exec,
                pod_name,
                self.namespace,
                command=command,
                container=container_name,  # Specify the container
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False
            )
            
            return resp
            
        except ApiException as e:
            self.logger.error(f"Failed to execute command in pod: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error executing command: {e}")
            return None

    def get_auth_token_for_command(self, pod: client.V1Pod, command_id: str) -> Optional[str]:
        """
        Get authentication token for a specific command based on its configuration
        
        Args:
            pod: V1Pod object
            command_id: ID of the command to get auth token for
            
        Returns:
            Authentication token if successful, None otherwise
        """
        # Look up the command configuration by command_id
        if not hasattr(self, 'commands') or command_id not in self.commands:
            self.logger.error(f"Command '{command_id}' not found in configuration")
            return None
        
        command_config = self.commands[command_id]
        
        # Determine which secret config to use based on command
        secret_config_key = command_config.get('secret_config') 
        if not secret_config_key:
            self.logger.error(f"No secret config specified for command '{command_id}'")
            return None      
        
        self.logger.info(f"Using secretConfig: {secret_config_key} for command '{command_id}'")
        
        # Check if we already have a valid token for this secret config
        token_cache_key = f"auth_token_{secret_config_key}"
        cached_token = self.global_variables.get(token_cache_key)
        
        if cached_token:
            # Check if token is still valid (simple check - you might want to add expiration logic)
            self.logger.debug(f"Using cached token for {secret_config_key}")
            return cached_token
        
        # Get fresh token - access the secret configs from the instance attributes
        if not hasattr(self, 'secret_configs') or secret_config_key not in self.secret_configs:
            self.logger.error(f"Secret config '{secret_config_key}' not found in configuration")
            return None
        
        secret_config = self.secret_configs[secret_config_key]
        
        try:
            # Get credentials from Kubernetes secret
            secret_name = secret_config['name']
            username_key = secret_config.get('username_key', 'username')
            password_key = secret_config.get('password_key', 'password')
            
            # Use the existing method to get credentials
            credentials = self._get_secret_credentials(secret_name, username_key, password_key)
            if not credentials:
                self.logger.error(f"Failed to retrieve credentials from secret '{secret_name}'")
                return None
            
            self.logger.info(f"Retrieved credentials from secret '{secret_name}' for {secret_config_key}")
            
            # Get token configuration and update with credentials
            token_config = self.token_config.copy()
            
            # Update credentials in token request
            if 'data' not in token_config:
                token_config['data'] = {}
            if 'auth' not in token_config['data']:
                token_config['data']['auth'] = {}
            if 'on-behalf-of' not in token_config['data']['auth']:
                token_config['data']['auth']['on-behalf-of'] = {}
            if 'user-credentials' not in token_config['data']['auth']['on-behalf-of']:
                token_config['data']['auth']['on-behalf-of']['user-credentials'] = {}
            
            token_config['data']['auth']['on-behalf-of']['user-credentials']['username'] = credentials['username']
            token_config['data']['auth']['on-behalf-of']['user-credentials']['password'] = credentials['password']
            
            # Make token request
            token_url = token_config['url']
            token_headers = token_config['headers']
            token_data = token_config['data']
            
            # Get the main container name
            container_name = self._get_main_container_name(pod.metadata.name)
            if not container_name:
                self.logger.error(f"Could not determine main container for pod {pod.metadata.name}")
                return None
            
            # Build curl command for token request
            curl_command = [
                'curl', '-X', 'POST',
                token_url,
                '--insecure',
                '--silent',
                '--show-error',
                '--fail-with-body',  # Show response body even on HTTP errors
                '--max-time', '30'
            ]
            
            # Add headers
            for header_name, header_value in token_headers.items():
                curl_command.extend(['-H', f'{header_name}: {header_value}'])
            
            # Add data            
            data_json = json.dumps(token_data)
            curl_command.extend(['--data-raw', data_json])
            
            # Execute token request using the existing method
            self.logger.info(f"Requesting token using {secret_config_key}")
            
            result = self.execute_command_in_pod(pod.metadata.name, curl_command, container_name)
            self.logger.debug(f"Token request result: {result}")
            
            if not result:
                self.logger.error(f"Token request failed for {secret_config_key}")
                return None
            
            # Parse token response with enhanced error handling
            try:
                # Clean kubectl warnings from the response
                cleaned_result = self._clean_kubectl_output(result)
                self.logger.debug(f"Cleaned response: {cleaned_result}")
                
                # Parse the response using the robust parser
                token_response = self._parse_response_robust(cleaned_result)
                
                if not token_response:
                    self.logger.error("Failed to parse token response")
                    return None
                
                self.logger.debug(f"Successfully parsed response: {list(token_response.keys())}")

                token_field = token_config.get('token_field', 'subjectToken')
                
                if token_field not in token_response:
                    self.logger.error(f"Token field '{token_field}' not found in response")
                    self.logger.debug(f"Available fields: {list(token_response.keys())}")
                    return None
                
                auth_token = f"Bearer {token_response[token_field]}"
                
                # Cache the token
                self.global_variables[token_cache_key] = auth_token
                
                self.logger.info(f"Successfully obtained auth token using {secret_config_key}")
                return auth_token
                    
            except Exception as e:
                self.logger.error(f"Failed to parse token response: {e}")
                self.logger.debug(f"Raw response: {result}")
                
                # Try to save the problematic response for debugging
                try:
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    debug_file = f'./logs/failed_token_response_{timestamp}.txt'
                    with open(debug_file, 'w') as f:
                        f.write("=== RAW RESPONSE ===\n")
                        f.write(result)
                        f.write("\n\n=== CLEANED RESPONSE ===\n")
                        f.write(cleaned_result)
                    self.logger.info(f"Saved problematic response to: {debug_file}")
                except Exception as save_error:
                    self.logger.error(f"Failed to save debug file: {save_error}")

                return None
                
        except Exception as e:
            self.logger.error(f"Failed to get auth token using {secret_config_key}: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def _substitute_variables(self, text: str, variables: Dict[str, str] = None) -> str:
        """
        Substitute variables in text using the format {variable_name}
        
        Args:
            text: Text containing variables to substitute
            variables: Dictionary of variables to substitute (optional, uses global_variables if not provided)
            
        Returns:
            Text with variables substituted
        """
        if not isinstance(text, str):
            return text
        
        # Use provided variables or fall back to global variables
        if variables is None:
            variables = self.global_variables
        
        if not variables:
            return text
        
        result = text
        for key, value in variables.items():
            placeholder = f"{{{key}}}"
            if placeholder in result:
                result = result.replace(placeholder, str(value))
                self.logger.debug(f"Substituted {placeholder} with {value}")
        
        return result

    def execute_curl_command(self, pod: client.V1Pod, curl_config: Dict[str, Any], auth_token: str = None) -> Optional[Dict[str, Any]]:
        """
        Execute a curl command in the specified pod with hooks support
        
        Args:
            pod: V1Pod object
            curl_config: Configuration for the curl command
            auth_token: Authentication token to use (optional, will be obtained if not provided)
            
        Returns:
            Response data if successful, None otherwise
        """
        command_id = curl_config.get('id', 'unknown')
        command_name = curl_config.get('name', 'Unnamed command')
        
        # Get auth token for this specific command if not provided
        if not auth_token:
            auth_token = self.get_auth_token_for_command(pod, command_id)
            if not auth_token:
                self.logger.error(f"Failed to obtain auth token for command '{command_id}'")
                return None
        
        # Prepare context for hooks
        context = {
            'command_id': command_id,
            'command_name': command_name,
            'command_config': curl_config,
            'auth_token': auth_token,
            'pod': pod,
            'executor': self,            
            'global_variables': self.global_variables,
            'response_store': self.response_store
        }
        
        # Add any custom context data from curl_config
        for key, value in curl_config.items():
            if key not in context:  # Don't overwrite existing context keys
                context[key] = value

        # Execute pre-hooks
        pre_hooks = curl_config.get('pre_hooks', [])
        if not self._execute_hooks('pre', pre_hooks, context):
            self.logger.error(f"Pre-hooks failed for command '{command_id}'")
            return None
        
        try:
            method = curl_config.get('method', 'GET')
            url = curl_config.get('url')
            headers = curl_config.get('headers', {})
            data = curl_config.get('data')
            
            if not url:
                self.logger.error("URL not specified in curl configuration")
                return None
                        
            # Build curl command with enhanced debugging and header capture
            curl_command = [
                'curl', '-X', method,
                url,
                '--insecure',
                '--silent',
                '--show-error',
                '--fail-with-body',  # Show response body even on HTTP errors
                '--header', 'Accept: application/json',
                '--location',    # Follow redirects
                '--max-time', '30',  # Set timeout
                '--connect-timeout', '10',  # Connection timeout
                '--include',  # Include response headers in output
                '--write-out', 'HTTPSTATUS:%{http_code}\\nEFFECTIVE_URL:%{url_effective}\\nTOTAL_TIME:%{time_total}\\n'  # Add status info
            ]

            # Add Accept header for JSON
            curl_command.extend(['-H', 'Accept: application/json']) 

             # If data is provided and method is POST/PUT/PATCH, substitute variables
            if data and method in ['POST', 'PUT', 'PATCH']:
                # Substitute variables in data
                if isinstance(data, dict):
                    # Convert dict to JSON string and substitute variables
                    data_str = json.dumps(data)
                    data_str = self._substitute_variables(data_str)
                    # Parse back to ensure it's valid JSON
                    try:
                        data = json.loads(data_str)
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Invalid JSON after variable substitution: {e}")
                        return None
                elif isinstance(data, str):
                    data = self._substitute_variables(data)
                
                # Print the constructed payload before making the call
                if isinstance(data, (dict, list)):                    
                    payload_json = json.dumps(data, indent=2)
                    self.logger.info(f"📤 Constructed payload for {command_name}:")
                    self.logger.info(f"\n{payload_json}")
                    data_json = json.dumps(data)
                    curl_command.extend(["--data-raw", data_json])
                else:
                    self.logger.info(f"📤 Constructed payload for {command_name}: {data}")
                    curl_command.extend(["--data-raw", str(data)])

            # Only add Content-Type for methods that typically have a body
            #if method.upper() in ['POST', 'PUT', 'PATCH']:
            #    curl_command.extend(['--header', 'Content-Type: application/json'])
            
            # Add authentication header
            curl_command.extend(['-H', f'Authorization: {auth_token}'])
                    

            # Add additional headers
            for header_name, header_value in headers.items():                
                curl_command.extend(['-H', f'{header_name}: {header_value}'])
            
            # Get the main container name
            container_name = self._get_main_container_name(pod.metadata.name)
            if not container_name:
                self.logger.error(f"Could not determine main container for pod {pod.metadata.name}")
                return None
            
            # Execute the command
            self.logger.info(f"🚀 Executing curl command for '{command_name}'")
            result = self.execute_command_in_pod(pod.metadata.name, curl_command, container_name)
            
            if result is None:
                self.logger.error(f"Failed to execute curl command for '{command_id}'")
                context['success'] = False
                context['response'] = None
                context['error'] = 'Command execution failed'
                self._execute_hooks('post', curl_config.get('post_hooks', []), context)
                return None
                                    
            # Parse the curl response to extract status, headers, and body
            response_info = self._parse_curl_response(result)
            
            # Check HTTP status code
            http_status = response_info.get('http_status', 0)
            is_success = 200 <= http_status < 300
            
            if is_success:
                self.logger.info(f"✅ Successfully executed command '{command_name}' (HTTP {http_status})")
                context['success'] = True
                context['response'] = response_info.get('body_parsed')
                context['http_status'] = http_status
                context['response_headers'] = response_info.get('headers', {})
                context['raw_response'] = result
            else:
                # Extract trace ID and other useful headers for error logging
                trace_id = response_info.get('headers', {}).get('x-dox-traceid', 'N/A')
                error_body = response_info.get('body_parsed', {})
                
                self.logger.error(f"❌ HTTP Error {http_status} for command '{command_name}'")
                self.logger.error(f"   Trace ID: {trace_id}")
                self.logger.error(f"   Error response: {error_body}")
                
                context['success'] = False
                context['response'] = error_body
                context['http_status'] = http_status
                context['response_headers'] = response_info.get('headers', {})
                context['trace_id'] = trace_id
                context['error'] = f'HTTP {http_status} error'
                context['raw_response'] = result
            
            # Execute post-hooks
            post_hooks = curl_config.get('post_hooks', [])
            self._execute_hooks('post', post_hooks, context)
            
            return context.get('response') if is_success else None
            
        except Exception as e:
            self.logger.error(f"Error executing curl command '{command_id}': {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            context['success'] = False
            context['response'] = None
            context['error'] = str(e)
            self._execute_hooks('post', curl_config.get('post_hooks', []), context)
            return None

    def _parse_curl_response(self, response: str) -> Dict[str, Any]:
        """
        Parse curl response to extract HTTP status, headers, and body
        
        Args:
            response: Raw curl response with headers and status info
            
        Returns:
            Dictionary containing parsed response information
        """
        if not response:
            return {'http_status': 0, 'headers': {}, 'body': '', 'body_parsed': None}
        
        # Clean kubectl warnings first
        cleaned_response = self._clean_kubectl_output(response)
        
        # Initialize result
        result = {
            'http_status': 0,
            'headers': {},
            'body': '',
            'body_parsed': None,
            'total_time': 0.0,
            'effective_url': ''
        }
        
        # Extract curl write-out information
        http_status_match = re.search(r'HTTPSTATUS:(\d+)', cleaned_response)
        if http_status_match:
            result['http_status'] = int(http_status_match.group(1))
        
        effective_url_match = re.search(r'EFFECTIVE_URL:([^\n]+)', cleaned_response)
        if effective_url_match:
            result['effective_url'] = effective_url_match.group(1)
        
        total_time_match = re.search(r'TOTAL_TIME:([0-9.]+)', cleaned_response)
        if total_time_match:
            result['total_time'] = float(total_time_match.group(1))
        
        # Remove curl write-out information from response
        cleaned_response = re.sub(r'HTTPSTATUS:\d+[^\n]*\n?', '', cleaned_response)
        cleaned_response = re.sub(r'EFFECTIVE_URL:[^\n]*\n?', '', cleaned_response)
        cleaned_response = re.sub(r'TOTAL_TIME:[0-9.]+[^\n]*\n?', '', cleaned_response)
        
        # Split response into lines
        lines = cleaned_response.split('\n')
        
        # Parse headers and find body start
        body_start_index = 0
        current_line = 0
        
        # Look for HTTP status line
        for i, line in enumerate(lines):
            if line.startswith('HTTP/'):
                current_line = i + 1
                break
        
        # Parse headers
        headers = {}
        for i in range(current_line, len(lines)):
            line = lines[i].strip()
            
            # Empty line indicates end of headers
            if not line:
                body_start_index = i + 1
                break
            
            # Parse header
            if ':' in line:
                key, value = line.split(':', 1)
                headers[key.strip().lower()] = value.strip()
        
        result['headers'] = headers
        
        # Extract body
        if body_start_index < len(lines):
            body_lines = lines[body_start_index:]
            body = '\n'.join(body_lines).strip()
            result['body'] = body
            
            # Try to parse body as JSON
            if body:
                try:
                    result['body_parsed'] = self._parse_response_robust(body)
                except Exception as e:
                    self.logger.debug(f"Failed to parse response body as JSON: {e}")
                    result['body_parsed'] = {'raw_body': body}
        
        return result


    def _clean_kubectl_output(self, output: str) -> str:
        """
        Clean kubectl warning messages from command output
        
        Args:
            output: Raw command output
            
        Returns:
            Cleaned output with kubectl warnings removed
        """
        if not output:
            return output
        
        # Remove common kubectl warning messages
        warnings_to_remove = [
            "Warning: Use tokens from the TokenRequest API or manually created secret-based tokens instead of auto-generated secret-based tokens.",
            "Warning: Use tokens from the TokenRequest API or manually created secret-based tokens instead of auto-generated secret-based tokens.\n",
            # Add other common warnings here if needed
        ]
        
        cleaned_output = output
        for warning in warnings_to_remove:
            self.logger.debug(f"Removing warning string: {warning}")
            cleaned_output = cleaned_output.replace(warning, "")
        
        # Also remove any leading/trailing whitespace and newlines
        cleaned_output = cleaned_output.strip()
        
        # Remove any remaining warning patterns that might have slight variations
        import re
        # Pattern to match kubectl warning messages
        warning_pattern = r'Warning: Use tokens from the TokenRequest API.*?tokens\.'
        cleaned_output = re.sub(warning_pattern, '', cleaned_output, flags=re.DOTALL)
        
        return cleaned_output.strip()

    def _parse_response_robust(self, response: str) -> Optional[Dict[str, Any]]:
        """
        Robust response parser that can handle various formats including Python dict literals
        
        Args:
            response: Response string to parse
            
        Returns:
            Parsed dictionary if successful, None otherwise
        """
        if not response or not response.strip():
            self.logger.error("Empty response provided")
            return None
        
        response = response.strip()
        
        # Strategy 1: Try standard JSON parsing first
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            self.logger.debug("Standard JSON parsing failed, trying alternative methods")
        
        # Strategy 2: Try Python literal evaluation (for dict-like responses with single quotes)
        try:
            import ast
            parsed_data = ast.literal_eval(response)
            if isinstance(parsed_data, dict):
                self.logger.info("Successfully parsed using Python literal evaluation")
                return parsed_data
        except (ValueError, SyntaxError) as e:
            self.logger.debug(f"Literal evaluation failed: {e}")
        
        # Strategy 3: Try to convert single quotes to double quotes for JSON
        try:
            import re
            
            # More comprehensive quote fixing
            fixed_response = response
            
            # Replace single quotes with double quotes around keys and values
            # This pattern is more careful about not replacing quotes inside strings
            fixed_response = re.sub(r"'([^']*?)'(\s*:)", r'"\1"\2', fixed_response)  # Keys
            fixed_response = re.sub(r":\s*'([^']*?)'", r': "\1"', fixed_response)    # Values
            
            # Handle boolean and null values that might be in Python format
            fixed_response = fixed_response.replace('True', 'true')
            fixed_response = fixed_response.replace('False', 'false')
            fixed_response = fixed_response.replace('None', 'null')
            
            parsed_data = json.loads(fixed_response)
            self.logger.info("Successfully recovered JSON by fixing quotes")
            return parsed_data
            
        except json.JSONDecodeError as e:
            self.logger.debug(f"Quote fixing strategy failed: {e}")
        
        # Strategy 4: Try to extract JSON from mixed content
        try:
            import re
            
            # Look for JSON object or array patterns
            json_pattern = r'(\{.*\}|\[.*\])'
            matches = re.findall(json_pattern, response, re.DOTALL)
            
            if matches:
                for match in matches:
                    try:
                        # Try literal eval first on extracted content
                        parsed_data = ast.literal_eval(match)
                        if isinstance(parsed_data, (dict, list)):
                            self.logger.info("Successfully extracted and parsed using literal evaluation")
                            return parsed_data
                    except:
                        try:
                            parsed_data = json.loads(match)
                            self.logger.info("Successfully extracted JSON from mixed content")
                            return parsed_data
                        except json.JSONDecodeError:
                            continue
                        
        except Exception as e:
            self.logger.debug(f"JSON extraction strategy failed: {e}")
        
        # Strategy 5: Manual parsing for the specific format we're seeing
        try:
            # If it looks like a Python dict, try to manually extract key-value pairs
            if response.startswith('{') and response.endswith('}'):
                self.logger.debug("Attempting manual dict parsing")
                
                # Remove outer braces
                content = response[1:-1].strip()
                
                # Simple regex to find key-value pairs
                # This is a simplified approach for the specific format we're seeing
                import re
                pattern = r"'([^']+)':\s*'([^']+)'"
                matches = re.findall(pattern, content)
                
                if matches:
                    result = {}
                    for key, value in matches:
                        result[key] = value
                    self.logger.info("Successfully parsed using manual dict extraction")
                    return result
                    
        except Exception as e:
            self.logger.debug(f"Manual parsing strategy failed: {e}")
        
        # All strategies failed
        self.logger.error("All parsing strategies failed")
        self.logger.error(f"Response format: {response[:200]}...")
        return None    
    
    def _improve_curl_response_format(self, curl_command: List[str]) -> List[str]:
        """
        Improve curl command to ensure JSON response format
        
        Args:
            curl_command: Original curl command list
            
        Returns:
            Modified curl command list with better JSON handling
        """
        # Add headers to ensure JSON response
        improved_command = curl_command.copy()
        
        # Ensure we're requesting JSON
        json_headers_added = False
        for i, arg in enumerate(improved_command):
            if arg == '-H' and i + 1 < len(improved_command):
                header = improved_command[i + 1]
                if 'Accept:' in header and 'application/json' in header:
                    json_headers_added = True
                    break
        
        if not json_headers_added:
            improved_command.extend(['-H', 'Accept: application/json'])
        
        # Add curl options to improve response handling
        additional_options = [
            '--compressed',  # Request compressed response
            '--http1.1',     # Force HTTP/1.1 to avoid potential HTTP/2 issues
        ]
        
        # Insert additional options after 'curl'
        for i, option in enumerate(additional_options):
            improved_command.insert(1 + i, option)
        
        return improved_command

    def _extract_json_from_curl_response(self, response: str) -> Optional[str]:
        """
        Extract JSON content from curl response that may include headers and status info
        
        Args:
            response: Raw curl response
            
        Returns:
            JSON content if found, None otherwise
        """
        if not response:
            return None
        
        lines = response.split('\n')
        json_started = False
        json_lines = []
        
        for line in lines:
            # Skip HTTP headers and status lines
            if line.startswith('HTTP/') or line.startswith('HTTPSTATUS:') or line.startswith('EFFECTIVE_URL:') or line.startswith('TOTAL_TIME:'):
                continue
            
            # Skip empty lines before JSON starts
            if not json_started and not line.strip():
                continue
            
            # Check if this line starts JSON content
            if not json_started and (line.strip().startswith('{') or line.strip().startswith('[')):
                json_started = True
            
            if json_started:
                json_lines.append(line)
        
        if json_lines:
            json_content = '\n'.join(json_lines).strip()
            self.logger.debug(f"Extracted JSON content: {json_content[:200]}...")
            return json_content
        
        return None
