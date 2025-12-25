from messaging.base_payload_generator import generate_read_payload

nodeid_list = ["ns=2;i=55", "ns=5;i=100"]
example_read_payload = generate_read_payload(nodeid_list=nodeid_list)
