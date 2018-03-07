pub enum PeeringConfig {
    Static,
    Dynamic,
}

pub enum SchedulerConfig {
    Serial,
    Parallel,
}

pub struct ValidatorConfig<'a> {
    pub bind_network: &'a str,
    pub bind_component: &'a str,
    pub endpoint: Option<&'a str>,

    pub peering: PeeringConfig,

    pub network_public_key: &'a str,
    pub network_private_key: &'a str,

    pub scheduler: SchedulerConfig,
    pub minimum_peer_connectivity: usize,
    pub maximum_peer_connectivity: usize,

}
