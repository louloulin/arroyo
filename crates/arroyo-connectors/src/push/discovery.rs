use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use anyhow::{Result, anyhow};
use serde::{Serialize, Deserialize};
use tracing::{debug, error, info};

/// Service registry for Push data plane services
pub struct PushServiceRegistry {
    /// Registered services
    services: RwLock<HashMap<String, ServiceInfo>>,
    
    /// Health check interval
    health_check_interval: Duration,
    
    /// Service timeout
    service_timeout: Duration,
}

impl PushServiceRegistry {
    /// Create a new service registry
    pub fn new() -> Self {
        Self {
            services: RwLock::new(HashMap::new()),
            health_check_interval: Duration::from_secs(30),
            service_timeout: Duration::from_secs(60),
        }
    }
    
    /// Create a new service registry with custom settings
    pub fn with_settings(health_check_interval: Duration, service_timeout: Duration) -> Self {
        Self {
            services: RwLock::new(HashMap::new()),
            health_check_interval,
            service_timeout,
        }
    }
    
    /// Register a service
    pub fn register_service(&self, service: ServiceInfo) -> Result<()> {
        let mut services = self.services.write().map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;
        services.insert(service.id.clone(), service);
        Ok(())
    }
    
    /// Unregister a service
    pub fn unregister_service(&self, service_id: &str) -> Result<()> {
        let mut services = self.services.write().map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;
        services.remove(service_id);
        Ok(())
    }
    
    /// Get a service by ID
    pub fn get_service(&self, service_id: &str) -> Result<ServiceInfo> {
        let services = self.services.read().map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        services.get(service_id).cloned().ok_or_else(|| anyhow!("Service not found: {}", service_id))
    }
    
    /// Get all services
    pub fn get_all_services(&self) -> Result<Vec<ServiceInfo>> {
        let services = self.services.read().map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        Ok(services.values().cloned().collect())
    }
    
    /// Get healthy services
    pub fn get_healthy_services(&self) -> Result<Vec<ServiceInfo>> {
        let services = self.services.read().map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        let now = SystemTime::now();
        
        Ok(services.values()
            .filter(|s| {
                if let Ok(elapsed) = now.duration_since(s.last_heartbeat) {
                    elapsed < self.service_timeout
                } else {
                    false
                }
            })
            .cloned()
            .collect())
    }
    
    /// Update service heartbeat
    pub fn update_heartbeat(&self, service_id: &str) -> Result<()> {
        let mut services = self.services.write().map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;
        
        if let Some(service) = services.get_mut(service_id) {
            service.last_heartbeat = SystemTime::now();
            service.status = ServiceStatus::Healthy;
            Ok(())
        } else {
            Err(anyhow!("Service not found: {}", service_id))
        }
    }
    
    /// Update service status
    pub fn update_status(&self, service_id: &str, status: ServiceStatus) -> Result<()> {
        let mut services = self.services.write().map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;
        
        if let Some(service) = services.get_mut(service_id) {
            service.status = status;
            Ok(())
        } else {
            Err(anyhow!("Service not found: {}", service_id))
        }
    }
    
    /// Check service health
    pub fn check_service_health(&self, service_id: &str) -> Result<ServiceStatus> {
        let services = self.services.read().map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        
        if let Some(service) = services.get(service_id) {
            let now = SystemTime::now();
            
            if let Ok(elapsed) = now.duration_since(service.last_heartbeat) {
                if elapsed > self.service_timeout {
                    Ok(ServiceStatus::Unhealthy)
                } else {
                    Ok(service.status.clone())
                }
            } else {
                Ok(ServiceStatus::Unknown)
            }
        } else {
            Err(anyhow!("Service not found: {}", service_id))
        }
    }
    
    /// Run health check on all services
    pub fn run_health_check(&self) -> Result<()> {
        let mut services = self.services.write().map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;
        let now = SystemTime::now();
        
        for service in services.values_mut() {
            if let Ok(elapsed) = now.duration_since(service.last_heartbeat) {
                if elapsed > self.service_timeout {
                    service.status = ServiceStatus::Unhealthy;
                }
            }
        }
        
        Ok(())
    }
    
    /// Start health check task
    pub fn start_health_check_task(&self) -> tokio::task::JoinHandle<()> {
        let registry = self.clone();
        let interval = self.health_check_interval;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            
            loop {
                interval.tick().await;
                
                if let Err(e) = registry.run_health_check() {
                    error!("Failed to run health check: {}", e);
                }
            }
        })
    }
}

impl Clone for PushServiceRegistry {
    fn clone(&self) -> Self {
        Self {
            services: RwLock::new(self.services.read().unwrap().clone()),
            health_check_interval: self.health_check_interval,
            service_timeout: self.service_timeout,
        }
    }
}

/// Service information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    /// Service ID
    pub id: String,
    
    /// Service name
    pub name: String,
    
    /// Service address
    pub address: String,
    
    /// Service port
    pub port: u16,
    
    /// Service protocols
    pub protocols: Vec<String>,
    
    /// Service status
    pub status: ServiceStatus,
    
    /// Last heartbeat time
    pub last_heartbeat: SystemTime,
    
    /// Service metadata
    pub metadata: HashMap<String, String>,
}

/// Service status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceStatus {
    /// Service is healthy
    Healthy,
    
    /// Service is unhealthy
    Unhealthy,
    
    /// Service status is unknown
    Unknown,
    
    /// Service is starting
    Starting,
    
    /// Service is stopping
    Stopping,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    
    #[test]
    fn test_service_registry_register() {
        let registry = PushServiceRegistry::new();
        
        let service = ServiceInfo {
            id: "service-1".to_string(),
            name: "Test Service".to_string(),
            address: "localhost".to_string(),
            port: 8000,
            protocols: vec!["http".to_string()],
            status: ServiceStatus::Healthy,
            last_heartbeat: SystemTime::now(),
            metadata: HashMap::new(),
        };
        
        let result = registry.register_service(service.clone());
        assert!(result.is_ok());
        
        let retrieved = registry.get_service("service-1");
        assert!(retrieved.is_ok());
        assert_eq!(retrieved.unwrap().id, "service-1");
    }
    
    #[test]
    fn test_service_registry_health_check() {
        let registry = PushServiceRegistry::with_settings(
            Duration::from_millis(100),
            Duration::from_millis(200),
        );
        
        let service = ServiceInfo {
            id: "service-1".to_string(),
            name: "Test Service".to_string(),
            address: "localhost".to_string(),
            port: 8000,
            protocols: vec!["http".to_string()],
            status: ServiceStatus::Healthy,
            last_heartbeat: SystemTime::now(),
            metadata: HashMap::new(),
        };
        
        let _ = registry.register_service(service);
        
        // Service should be healthy initially
        let status = registry.check_service_health("service-1").unwrap();
        assert_eq!(status, ServiceStatus::Healthy);
        
        // Wait for service to become unhealthy
        thread::sleep(Duration::from_millis(300));
        
        // Run health check
        let _ = registry.run_health_check();
        
        // Service should be unhealthy now
        let status = registry.check_service_health("service-1").unwrap();
        assert_eq!(status, ServiceStatus::Unhealthy);
    }
}
