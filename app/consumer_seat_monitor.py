import sys
import json
from kafka import KafkaConsumer
from rich import print as rprint
from config import TOPIC_NAME, BOOTSTRAP_SERVERS, COURSES

GROUP_ID = sys.argv[1] if len(sys.argv) > 1 else "seat-monitor-group"
WAITING_CAPACITY = 5  # 대기열 최대 인원 (이 이상은 신청 실패 처리)

# 과목별 현재 신청/대기 인원 추적
seats = {cid: {"enrolled": [], "waiting": []} for cid in COURSES}


def process(student, course_id):
    course_name = COURSES[course_id]["name"]
    cap = COURSES[course_id]["capacity"]
    state = seats[course_id]

    if len(state["enrolled"]) < cap:
        state["enrolled"].append(student)
        rprint(f"[bold green]{student} - {course_name} 수강신청 성공![/bold green] ({len(state['enrolled'])}/{cap})")

    elif len(state["waiting"]) < WAITING_CAPACITY:
        state["waiting"].append(student)
        rprint(f"[bold yellow]{student} - {course_name} 대기 {len(state['waiting'])}번[/bold yellow]")

    else:
        rprint(f"[bold red]{student} - {course_name} 수강신청 실패..[/bold red]")


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    rprint(f"[bold cyan][INFO] 정원 판정 시작 (Group: {GROUP_ID}, 대기열 정원: {WAITING_CAPACITY}명)[/bold cyan]")

    for message in consumer:
        log = message.value
        process(log["student_id"], log["course_id"])
        consumer.commit()  # 판정 완료 후 수동 커밋


if __name__ == "__main__":
    main()