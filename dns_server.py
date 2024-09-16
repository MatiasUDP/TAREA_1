import grpc
from concurrent import futures
import subprocess
import dns_service_pb2
import dns_service_pb2_grpc

class DNSService(dns_service_pb2_grpc.DNSServiceServicer):
    def Lookup(self, request, context):
        domain = request.domain
        try:
            result = subprocess.run(
                ['dig', '+short', domain],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True
            )
            ip_address = result.stdout.decode().strip()
            return dns_service_pb2.DNSResponse(ip_address=ip_address)
        except subprocess.CalledProcessError:
            return dns_service_pb2.DNSResponse(ip_address="")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dns_service_pb2_grpc.add_DNSServiceServicer_to_server(DNSService(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("DNS server started on port 50052")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
