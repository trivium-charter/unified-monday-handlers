import os
import json
from datetime import datetime
import pytz 
from celery_app import celery_app
import monday_utils as monday
import canvas_utils as canvas

# --- Environment variables ---
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID")
ALL_CLASSES_BOARD_ID = os.environ.get("ALL_CLASSES_BOARD_ID")
CANVAS_CLASSES_BOARD_ID = os.environ.get("CANVAS_CLASSES_BOARD_ID")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "")
PLP_CANVAS_SYNC_STATUS_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_STATUS_COLUMN_ID")
PLP_CANVAS_SYNC_STATUS_VALUE = os.environ.get("PLP_CANVAS_SYNC_STATUS_VALUE")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN")
ALL_CLASSES_CANVAS_CONNECT_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_CONNECT_COLUMN")
CANVAS_COURSE_ID_COLUMN = os.environ.get("CANVAS_COURSE_ID_COLUMN")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN")
PLP_OP2_SECTION_COLUMN = os.environ.get("PLP_OP2_SECTION_COLUMN")
PLP_M_SERIES_LABELS_COLUMN = os.environ.get("PLP_M_SERIES_LABELS_COLUMN") 
CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")

# --- Celery Task ---
@celery_app.task
def process_canvas_sync_webhook(event_data):
    """
    Handles syncing a student's enrollments in Canvas based on a webhook trigger.
    """
    print("INFO: CANVAS_SYNC - Task started.")
    plp_item_id = event_data.get('pulseId')
    trigger_column_id = event_data.get('columnId')
    
    # 1. Get Student Information
    student_name = monday.get_item_name(plp_item_id, PLP_BOARD_ID)
    master_student_ids = monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)
    if not master_student_ids:
        print(f"ERROR: PLP item {plp_item_id} not linked to a Master Student.")
        return False
    master_student_id = list(master_student_ids)[0]
    
    student_email = (monday.get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_EMAIL_COLUMN) or {}).get('text')
    student_ssid = (monday.get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_SSID_COLUMN) or {}).get('text')

    if not all([student_email, student_name]):
        print(f"ERROR: Student email or name not found for PLP item {plp_item_id}.")
        return False
        
    if not student_ssid or not student_ssid.strip():
        student_ssid = f"monday_{plp_item_id}"

    student_details = {"name": student_name, "email": student_email, "ssid": student_ssid}
    print(f"INFO: Syncing for student: {student_details}")

    # 2. Determine Classes to Sync
    plp_connect_cols = [col.strip() for col in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',')]
    linked_class_ids, unlinked_class_ids = set(), set()

    if trigger_column_id in plp_connect_cols:
        linked_class_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
        unlinked_class_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue')) - linked_class_ids
    elif trigger_column_id == PLP_CANVAS_SYNC_STATUS_COLUMN_ID:
        for col_id in plp_connect_cols:
            linked_class_ids.update(monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, col_id))
    
    print(f"INFO: Classes to enroll: {linked_class_ids}")
    print(f"INFO: Classes to unenroll: {unlinked_class_ids}")

    # 3. Process Enrollments
    for class_item_id in linked_class_ids:
        print(f"--- Processing Enrollment for Class ID: {class_item_id} ---")
        
        canvas_class_link_ids = monday.get_linked_items_from_board_relation(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN)
        if not canvas_class_link_ids:
            print(f"INFO: Class item {class_item_id} is not linked to the 'Canvas Classes' board. Skipping.")
            continue
        
        canvas_class_item_id = list(canvas_class_link_ids)[0]
        print(f"INFO: Checking for existing Canvas Course ID on item {canvas_class_item_id}...")
        canvas_course_id = (monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_ID_COLUMN) or {}).get('text')

        if not canvas_course_id or not canvas_course_id.strip():
            print(f"INFO: Canvas Course ID is blank. Attempting to create a new course.")
            class_item_name = monday.get_item_name(class_item_id, ALL_CLASSES_BOARD_ID) or "Untitled"
            
            if not CANVAS_TERM_ID:
                print(f"ERROR: CANVAS_TERM_ID not set. Cannot create course.")
                continue
                
            new_course = canvas.create_canvas_course(class_item_name, CANVAS_TERM_ID)
            
            if new_course:
                canvas_course_id = new_course.id
                print(f"INFO: New course created with ID: {canvas_course_id}. Updating Monday.com.")
                monday.change_column_value_generic(CANVAS_CLASSES_BOARD_ID, canvas_class_item_id, CANVAS_COURSE_ID_COLUMN, str(canvas_course_id))
            else:
                print(f"ERROR: Course creation for '{class_item_name}' failed or was aborted. Skipping.")
                continue
        else:
            print(f"INFO: Found existing Canvas Course ID: {canvas_course_id}.")
        
        sections_to_enroll = set()
        ag_grad_text = (monday.get_column_value(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_AG_GRAD_COLUMN) or {}).get('text', '')
        if "AG" in ag_grad_text: sections_to_enroll.add("A-G")
        if "Grad" in ag_grad_text: sections_to_enroll.add("Grad")

        if (monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_OP2_SECTION_COLUMN) or {}).get('text') == "Op2 Section":
            sections_to_enroll.add("Op2")
        
        labels_text = (monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_M_SERIES_LABELS_COLUMN) or {}).get('text', '')
        for m_label in ["M2", "M3", "M4", "M5"]:
            if m_label in labels_text:
                sections_to_enroll.add(m_label)
        
        for section_name in sections_to_enroll:
            section = canvas.create_section_if_not_exists(canvas_course_id, section_name)
            if section:
                canvas.enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)

    # 4. Process Unenrollments
    for class_item_id in unlinked_class_ids:
        print(f"--- Processing Unenrollment for Class ID: {class_item_id} ---")
        canvas_class_link_ids = monday.get_linked_items_from_board_relation(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN)
        if not canvas_class_link_ids: continue
        
        canvas_class_item_id = list(canvas_class_link_ids)[0]
        canvas_course_id = (monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_ID_COLUMN) or {}).get('text')

        if canvas_course_id and canvas_course_id.strip():
            canvas.unenroll_student_from_course(canvas_course_id, student_email)

    print("INFO: CANVAS_SYNC - Task finished.")
    return True
