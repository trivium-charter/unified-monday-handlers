import os
import json
from datetime import datetime
import pytz 
from celery_app import celery_app
import monday_utils as monday
import canvas_utils as canvas

# --- Global Configuration (Environment Variables for Tasks) ---

# --- Environment variables for Canvas Sync Process ---
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID", "8993025745")
ALL_CLASSES_BOARD_ID = os.environ.get("ALL_CLASSES_BOARD_ID", "8931036662")
CANVAS_CLASSES_BOARD_ID = os.environ.get("CANVAS_CLASSES_BOARD_ID", "7308051382")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "board_relation_mkqnbtaf,board_relation_mkqnxyjd,board_relation_mkqn34pg,board_relation_mkr54dtg")
PLP_CANVAS_SYNC_STATUS_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_STATUS_COLUMN_ID", "color_mktdzdxj")
PLP_CANVAS_SYNC_STATUS_VALUE = os.environ.get("PLP_CANVAS_SYNC_STATUS_VALUE", "Sync")
PLP_STUDENT_EMAIL_COLUMN = os.environ.get("PLP_STUDENT_EMAIL_COLUMN", "email1__1")
PLP_STUDENT_SSID_COLUMN = os.environ.get("PLP_STUDENT_SSID_COLUMN", "lookup_mktdc1ky")
ALL_CLASSES_CANVAS_CONNECT_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_CONNECT_COLUMN", "board_relation_mkt2hp4c")
CANVAS_COURSE_ID_COLUMN = os.environ.get("CANVAS_COURSE_ID_COLUMN", "canvas_course_id_mkm1fwt4")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN", "dropdown_mkq0r2sj")
PLP_OP2_SECTION_COLUMN = os.environ.get("PLP_OP2_SECTION_COLUMN", "lookup_mkta9mgv")
PLP_M_SERIES_LABELS_COLUMN = os.environ.get("PLP_M_SERIES_LABELS_COLUMN", "labels_mktXXXX") 
CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")

# (The rest of your existing environment variable loading for other tasks can remain)
# ...

# --- Celery Tasks ---

# (Your existing tasks: process_general_webhook, etc. remain here unchanged)
# ...

@celery_app.task
def process_canvas_sync_webhook(event_data):
    """
    Handles the full sync of a student's enrollments and sections in Canvas,
    creating the student if they do not exist.
    """
    print("INFO: CANVAS_SYNC - Task started.")
    plp_item_id = event_data.get('pulseId')
    trigger_column_id = event_data.get('columnId')
    
    # --- 1. Get All Student Information ---
    student_email_val = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_STUDENT_EMAIL_COLUMN)
    student_email = student_email_val.get('text') if student_email_val else None
    
    student_ssid_val = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_STUDENT_SSID_COLUMN)
    student_ssid = student_ssid_val.get('text') if student_ssid_val else None
    
    student_name = monday.get_item_name(plp_item_id, PLP_BOARD_ID)

    if not all([student_email, student_name]):
        print(f"ERROR: CANVAS_SYNC - Student email or name not found on PLP item {plp_item_id}. Aborting.")
        return False
        
    if not student_ssid or not student_ssid.strip():
        student_ssid = f"monday_{plp_item_id}"
        print(f"WARNING: CANVAS_SYNC - SSID is blank. Using fallback SIS ID: {student_ssid}")

    student_details = {
        "name": student_name,
        "email": student_email,
        "ssid": student_ssid
    }
    print(f"INFO: CANVAS_SYNC - Syncing for student: {student_details}")

    # --- 2. Determine which classes to sync ---
    plp_connect_cols = [col.strip() for col in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',')]
    
    linked_class_ids = set()
    unlinked_class_ids = set()

    if trigger_column_id in plp_connect_cols:
        current_val = event_data.get('value')
        previous_val = event_data.get('previousValue')
        linked_class_ids = monday.get_linked_ids_from_connect_column_value(current_val)
        unlinked_class_ids = monday.get_linked_ids_from_connect_column_value(previous_val) - linked_class_ids
    elif trigger_column_id == PLP_CANVAS_SYNC_STATUS_COLUMN_ID:
        for col_id in plp_connect_cols:
            linked_ids = monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, col_id)
            linked_class_ids.update(linked_ids)
    
    print(f"INFO: CANVAS_SYNC - Classes to sync/enroll: {linked_class_ids}")
    print(f"INFO: CANVAS_SYNC - Classes to unenroll: {unlinked_class_ids}")

    # --- 3. Process Classes to Sync/Enroll ---
    for class_item_id in linked_class_ids:
        print(f"--- Processing Class ID: {class_item_id} ---")
        canvas_class_link_ids = monday.get_linked_items_from_board_relation(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN)
        if not canvas_class_link_ids:
            print(f"INFO: Class {class_item_id} is not a Canvas class. Skipping.")
            continue
        
        canvas_class_item_id = list(canvas_class_link_ids)[0]
        canvas_course_id_val = monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_ID_COLUMN)
        canvas_course_id = canvas_course_id_val.get('text') if canvas_course_id_val else None

        if not canvas_course_id:
            class_item_name = monday.get_item_name(class_item_id, ALL_CLASSES_BOARD_ID) or "Untitled Canvas Course"
            if not CANVAS_TERM_ID:
                print("ERROR: CANVAS_SYNC - CANVAS_TERM_ID not set. Cannot create course.")
                continue
            new_course = canvas.create_canvas_course(class_item_name, CANVAS_TERM_ID)
            if new_course:
                canvas_course_id = new_course.id
                monday.change_column_value_generic(CANVAS_CLASSES_BOARD_ID, canvas_class_item_id, CANVAS_COURSE_ID_COLUMN, str(canvas_course_id))
            else:
                print(f"ERROR: Failed to create Canvas course for class {class_item_id}. Skipping.")
                continue
        
        sections_to_enroll = set()
        ag_grad_val = monday.get_column_value(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_AG_GRAD_COLUMN)
        ag_grad_text = ag_grad_val.get('text') if ag_grad_val else ""
        if "AG" in ag_grad_text: sections_to_enroll.add("A-G")
        if "Grad" in ag_grad_text: sections_to_enroll.add("Grad")

        op2_val = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_OP2_SECTION_COLUMN)
        if op2_val and op2_val.get('text') == "Op2 Section": sections_to_enroll.add("Op2")
        
        labels_val = monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_M_SERIES_LABELS_COLUMN)
        if labels_val and labels_val.get('text'):
            labels_text = labels_val.get('text')
            for m_label in ["M2", "M3", "M4", "M5"]:
                if m_label in labels_text:
                    sections_to_enroll.add(m_label)

        print(f"INFO: Required sections for course {canvas_course_id}: {sections_to_enroll}")

        for section_name in sections_to_enroll:
            section = canvas.create_section_if_not_exists(canvas_course_id, section_name)
            if section:
                canvas.enroll_or_create_and_enroll(canvas_course_id, section.id, student_details)
            else:
                print(f"ERROR: Could not find or create section '{section_name}'. Skipping enrollment.")

    # --- 4. Process Classes to Unenroll ---
    for class_item_id in unlinked_class_ids:
        canvas_class_link_ids = monday.get_linked_items_from_board_relation(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN)
        if not canvas_class_link_ids:
            continue
        
        canvas_class_item_id = list(canvas_class_link_ids)[0]
        canvas_course_id_val = monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_ID_COLUMN)
        canvas_course_id = canvas_course_id_val.get('text') if canvas_course_id_val else None

        if canvas_course_id:
            print(f"INFO: CANVAS_SYNC - Deactivating enrollment for {student_email} in course {canvas_course_id}.")
            canvas.unenroll_student_from_course(canvas_course_id, student_email)

    print("INFO: CANVAS_SYNC - Task finished.")
    return True
