import json
from kafka import KafkaConsumer
from config import TOPIC_NAME, BOOTSTRAP_SERVERS, COURSES

# seat-monitor-group과 별개의 그룹 → 완전히 독립적으로 동작
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="log-storage-group",
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("[INFO] 모든 수강신청 로그를 백업 저장합니다 (Group: log-storage-group)")

for message in consumer:
    log = message.value
    course_name = COURSES.get(log["course_id"], {}).get("name", log["course_id"])
    print(f"[BACKUP] (P{message.partition}) {log['ts']} | {log['student_id']} → {course_name} 신청 기록 완료.")