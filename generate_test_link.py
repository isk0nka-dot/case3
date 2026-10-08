import jwt
import uuid
import time
import urllib.parse
import psycopg2
from datetime import datetime, timedelta

secret = "argus-dev-admin-jwt-key-CHANGE-IN-PRODUCTION!"
session_id = str(uuid.uuid4())
now = int(time.time())

payload = {
    "iss": "argus-external-api",
    "aud": ["argus-event-collector"],
    "exp": now + 3600*24,
    "iat": now,
    "sessionId": session_id,
    "studentId": "demo-student-123",
    "examId": "demo-exam",
    "orgId": "org-eduser",
    "roles": ["student"]
}

token = jwt.encode(payload, secret, algorithm="HS256")

# Insert the session into the database so it appears in the dashboard
try:
    conn = psycopg2.connect('postgresql://argus:argus_secret@localhost:5435/argus')
    conn.autocommit = True
    cur = conn.cursor()
    expires_at = datetime.now() + timedelta(days=1)
    
    cur.execute(
        """
        INSERT INTO external_sessions 
        (session_id, org_id, exam_id, student_id, student_name, exam_name, session_token, token_expires_at, status, started_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            session_id, 
            "org-eduser", 
            "demo-exam", 
            "demo-student-123", 
            "Студент (Демо)", 
            "Демонстрациялық емтихан", 
            token, 
            expires_at, 
            "in_progress",
            datetime.now()
        )
    )
    print("✅ Сессия дерекқорға сәтті қосылды! (Dashboard-тан көрінуі тиіс)")
except Exception as e:
    print(f"⚠️ Сессияны дерекқорға қосу кезінде қате: {e}")

url = f"http://localhost:3000/exam.html?token={token}&serverUrl=http://localhost:8080"

print("\n--- ДЕМО СІЛТЕМЕ (ЭКЗАМЕН) ---")
print(url)
print("------------------------------\n")
