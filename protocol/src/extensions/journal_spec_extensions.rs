/// Journal uniquely identifies a journal brokered by Gazette.
/// By convention, journals are named using a forward-slash notation which
/// captures their hierarchical relationships into organizations, topics and
/// partitions. For example, a Journal might be:
/// "company-journals/interesting-topic/part-1234"
pub type Journal = String;
