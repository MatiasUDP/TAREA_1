import grpc
import dns_service_pb2
import dns_service_pb2_grpc

def lookup_dns(domain):
    with grpc.insecure_channel('localhost:50052') as channel:
        stub = dns_service_pb2_grpc.DNSServiceStub(channel)
        response = stub.Lookup(dns_service_pb2.DNSRequest(domain=domain))
        return response.ip_address

if __name__ == '__main__':
    domain = 'example.com'  # Cambia esto al dominio que deseas consultar
    ip_address = lookup_dns(domain)
    print(f"IP Address for {domain}: {ip_address}")
