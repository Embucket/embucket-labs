#!/usr/bin/env python3
"""
Docker management utilities for Embucket benchmark infrastructure.
Handles SSH connections to EC2 instance and Docker Compose operations.
"""

import os
import time
import subprocess
import requests
import logging
from typing import Optional, Tuple
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DockerManager:
    """Manages Docker operations on remote EC2 instance via SSH."""
    
    def __init__(self):
        """Initialize Docker manager with environment configuration."""
        # Use EMBUCKET_HOST for both SSH connection and API URL
        # When running locally, EMBUCKET_HOST=localhost
        # When running against EC2, EMBUCKET_HOST=<ec2_public_ip>
        self.embucket_host = os.getenv("EMBUCKET_HOST", "localhost")
        self.ec2_user = os.getenv("EC2_USER", "ec2-user")
        self.ssh_key_path = os.getenv("SSH_KEY_PATH", "~/.ssh/id_rsa")
        self.embucket_port = os.getenv("EMBUCKET_PORT", "3000")
        self.embucket_url = f"http://{self.embucket_host}:{self.embucket_port}"

        # Retry configuration
        self.max_retries = 30
        self.retry_delay = 10  # seconds
        self.health_check_timeout = 5  # seconds

        if not self.embucket_host:
            raise ValueError("EMBUCKET_HOST environment variable is required")

        logger.info(f"Initialized DockerManager for {self.embucket_host}")
    
    def _run_ssh_command(self, command: str, timeout: int = 60) -> Tuple[bool, str]:
        """Execute command on remote EC2 instance via SSH."""
        ssh_command = [
            "ssh",
            "-i", os.path.expanduser(self.ssh_key_path),
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            f"{self.ec2_user}@{self.embucket_host}",
            command
        ]
        
        try:
            logger.debug(f"Executing SSH command: {' '.join(ssh_command)}")
            result = subprocess.run(
                ssh_command,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            success = result.returncode == 0
            output = result.stdout if success else result.stderr
            
            if success:
                logger.debug(f"SSH command succeeded: {output.strip()}")
            else:
                logger.error(f"SSH command failed (code {result.returncode}): {output.strip()}")
                
            return success, output.strip()
            
        except subprocess.TimeoutExpired:
            logger.error(f"SSH command timed out after {timeout} seconds")
            return False, f"Command timed out after {timeout} seconds"
        except Exception as e:
            logger.error(f"SSH command failed with exception: {e}")
            return False, str(e)
    
    def check_embucket_health(self) -> bool:
        """Check if Embucket API is responding to health checks."""
        try:
            logger.debug(f"Checking Embucket health at {self.embucket_url}/health")
            response = requests.get(
                f"{self.embucket_url}/health",
                timeout=self.health_check_timeout
            )
            
            is_healthy = response.status_code == 200
            if is_healthy:
                logger.debug("Embucket health check passed")
            else:
                logger.debug(f"Embucket health check failed with status {response.status_code}")
                
            return is_healthy
            
        except requests.exceptions.RequestException as e:
            logger.debug(f"Embucket health check failed: {e}")
            return False
    
    def wait_for_embucket_ready(self) -> bool:
        """Wait for Embucket to be ready after restart."""
        logger.info(f"Waiting for Embucket to be ready (max {self.max_retries} attempts)")
        
        for attempt in range(self.max_retries):
            if self.check_embucket_health():
                logger.info(f"Embucket is ready after {attempt + 1} attempts")
                return True
                
            if attempt < self.max_retries - 1:
                logger.debug(f"Attempt {attempt + 1}/{self.max_retries}: Embucket not ready, waiting {self.retry_delay}s")
                time.sleep(self.retry_delay)
        
        logger.error(f"Embucket failed to become ready after {self.max_retries} attempts")
        return False
    
    def get_container_status(self) -> Tuple[bool, str]:
        """Get status of Embucket container."""
        success, output = self._run_ssh_command("cd /home/ec2-user && docker-compose ps embucket")
        return success, output
    
    def stop_embucket_container(self) -> bool:
        """Stop the Embucket container."""
        logger.info("Stopping Embucket container...")
        success, output = self._run_ssh_command("cd /home/ec2-user && docker-compose stop embucket", timeout=120)
        
        if success:
            logger.info("Embucket container stopped successfully")
        else:
            logger.error(f"Failed to stop Embucket container: {output}")
            
        return success
    
    def start_embucket_container(self) -> bool:
        """Start the Embucket container."""
        logger.info("Starting Embucket container...")
        success, output = self._run_ssh_command("cd /home/ec2-user && docker-compose start embucket", timeout=120)
        
        if success:
            logger.info("Embucket container started successfully")
        else:
            logger.error(f"Failed to start Embucket container: {output}")
            
        return success
    
    def restart_embucket_container(self) -> bool:
        """Restart the Embucket container and wait for it to be ready."""
        start_time = time.time()
        logger.info("Starting Embucket container restart process...")
        
        # Stop container
        if not self.stop_embucket_container():
            return False
        
        # Wait a moment for clean shutdown
        time.sleep(5)
        
        # Start container
        if not self.start_embucket_container():
            return False
        
        # Wait for container to be ready
        if not self.wait_for_embucket_ready():
            return False
        
        restart_time = time.time() - start_time
        logger.info(f"Embucket container restart completed successfully in {restart_time:.2f} seconds")
        return True
    
    def get_container_logs(self, lines: int = 50) -> Tuple[bool, str]:
        """Get recent logs from Embucket container."""
        success, output = self._run_ssh_command(
            f"cd /home/ec2-user && docker-compose logs --tail={lines} embucket"
        )
        return success, output
    
    def cleanup_docker_resources(self) -> bool:
        """Clean up Docker resources (containers, networks, volumes)."""
        logger.info("Cleaning up Docker resources...")
        
        commands = [
            "cd /home/ec2-user && docker-compose down",
            "docker system prune -f",
            "docker volume prune -f"
        ]
        
        for command in commands:
            success, output = self._run_ssh_command(command, timeout=180)
            if not success:
                logger.warning(f"Docker cleanup command failed: {command} - {output}")
                return False
        
        logger.info("Docker resources cleaned up successfully")
        return True


def create_docker_manager() -> DockerManager:
    """Factory function to create DockerManager instance."""
    return DockerManager()


# Convenience functions for direct use
def restart_embucket() -> bool:
    """Convenience function to restart Embucket container."""
    manager = create_docker_manager()
    return manager.restart_embucket_container()


def check_embucket_status() -> Tuple[bool, str]:
    """Convenience function to check Embucket container status."""
    manager = create_docker_manager()
    return manager.get_container_status()


if __name__ == "__main__":
    # Test the Docker manager
    manager = create_docker_manager()
    
    print("Testing Docker manager...")
    print(f"Embucket Host: {manager.embucket_host}")
    print(f"Embucket URL: {manager.embucket_url}")
    
    # Check current status
    success, status = manager.get_container_status()
    print(f"Container status: {status}")
    
    # Test health check
    is_healthy = manager.check_embucket_health()
    print(f"Health check: {'PASS' if is_healthy else 'FAIL'}")
