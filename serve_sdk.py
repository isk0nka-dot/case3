import http.server
import socketserver
import os

class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()

PORT = 8081
os.chdir('argus-sdk/dist')
with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"Serving SDK at http://localhost:{PORT}")
    httpd.serve_forever()

