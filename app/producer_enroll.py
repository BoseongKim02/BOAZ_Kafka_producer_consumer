import json
import time
import random
from kafka import KafkaProducer
from config import TOPIC_NAME, BOOTSTRAP_SERVERS, COURSES, STUDENTS

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# 1. 가능한 모든 (학생, 과목) 조합 생성 — 중복 신청 원천 차단
remaining_pairs = [
    (student, course_id)
    for student in STUDENTS
    for course_id in COURSES.keys()
]
random.shuffle(remaining_pairs)

print("==========================================")
print(f"🎓 수강신청 시작!")
print(f"참여 학생: {len(STUDENTS)}명 / 개설 과목: {len(COURSES)}개")
print(f"총 신청 가능 건수: {len(remaining_pairs)}건")
print("==========================================")

try:
    while remaining_pairs:
        # 2. 중복 없는 조합을 하나 꺼내서 제거
        student, course_id = remaining_pairs.pop()
        course_name = COURSES[course_id]["name"]

        data = {
            "student_id": student,
            "course_id": course_id,
            "ts": time.strftime('%H:%M:%S')
        }

        # 3. 카프카 전송 (key=course_id → 같은 과목은 항상 같은 파티션으로,
        #    선착순 순서 보장)
        producer.send(
            TOPIC_NAME,
            key=course_id.encode('utf-8'),
            value=data
        )

        print(f"[{data['ts']}] 🖱️  {student:.<8} → {course_name}({course_id}) 신청")

        time.sleep(0.5)

    print("\n[INFO] 모든 학생이 모든 과목 신청을 마쳤습니다. 수강신청 종료.")

except KeyboardInterrupt:
    print(f"\n[INFO] 신청 중단. 남은 미신청 건수: {len(remaining_pairs)}건")

finally:
    producer.flush()
    producer.close()