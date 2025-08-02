import os
import json
from datetime import datetime
import pytz 
from celery_app import celery_app
import monday_utils as monday
import canvas_utils as canvas

# --- Global Configuration with Defaults ---
PLP_BOARD_ID = os.environ.get("PLP_BOARD_ID", "8993025745")
ALL_CLASSES_BOARD_ID = os.environ.get("ALL_CLASSES_BOARD_ID", "8931036662")
CANVAS_CLASSES_BOARD_ID = os.environ.get("CANVAS_CLASSES_BOARD_ID", "7308051382")
MASTER_STUDENT_BOARD_ID = os.environ.get("MASTER_STUDENT_BOARD_ID", "6563671510")
HS_COURSE_ROSTER_BOARD_ID = os.environ.get("HS_COURSE_ROSTER_BOARD_ID", "8792275301")
PLP_ALL_CLASSES_CONNECT_COLUMNS_STR = os.environ.get("PLP_ALL_CLASSES_CONNECT_COLUMNS_STR", "board_relation_mkqnbtaf,board_relation_mkqnxyjd,board_relation_mkqn34pg,board_relation_mkr54dtg")
PLP_CANVAS_SYNC_STATUS_COLUMN_ID = os.environ.get("PLP_CANVAS_SYNC_STATUS_COLUMN_ID", "color_mktdzdxj")
PLP_TO_MASTER_STUDENT_CONNECT_COLUMN = os.environ.get("PLP_TO_MASTER_STUDENT_CONNECT_COLUMN", "board_relation_mks1n32a")
MASTER_STUDENT_SSID_COLUMN = os.environ.get("MASTER_STUDENT_SSID_COLUMN", "text__1")
MASTER_STUDENT_EMAIL_COLUMN = os.environ.get("MASTER_STUDENT_EMAIL_COLUMN", "_students1__school_email_address")
ALL_CLASSES_CANVAS_CONNECT_COLUMN = os.environ.get("ALL_CLASSES_CANVAS_CONNECT_COLUMN", "board_relation_mkt2hp4c")
CANVAS_COURSE_ID_COLUMN = os.environ.get("CANVAS_COURSE_ID_COLUMN", "canvas_course_id_mkm1fwt4")
CANVAS_COURSE_TITLE_COLUMN = os.environ.get("CANVAS_COURSE_TITLE_COLUMN", "text65__1")
ALL_CLASSES_AG_GRAD_COLUMN = os.environ.get("ALL_CLASSES_AG_GRAD_COLUMN", "dropdown_mkq0r2sj")
PLP_OP2_SECTION_COLUMN = os.environ.get("PLP_OP2_SECTION_COLUMN", "lookup_mkta9mgv")
CANVAS_TERM_ID = os.environ.get("CANVAS_TERM_ID")
MASTER_STUDENT_LIST_BOARD_ID = os.environ.get("MASTER_STUDENT_LIST_BOARD_ID", "6563671510")
SPED_STUDENTS_BOARD_ID = os.environ.get("SPED_STUDENTS_BOARD_ID", "6760943570")
IEP_AP_BOARD_ID = os.environ.get("IEP_AP_BOARD_ID", "6760108968")
MASTER_STUDENT_PEOPLE_COLUMNS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMNS", '{}'))
SPED_STUDENTS_PEOPLE_COLUMN = json.loads(os.environ.get("SPED_STUDENTS_PEOPLE_COLUMN", '{}'))
COLUMN_MAPPINGS = json.loads(os.environ.get("MASTER_STUDENT_PEOPLE_COLUMN_MAPPINGS", '{}'))
SPED_TO_IEPAP_CONNECT_COLUMN = os.environ.get("SPED_TO_IEPAP_CONNECT_COLUMN", "board_relation1__1")

@celery_app.task
def process_general_webhook(event_data, config_rule):
    pass

@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    pass

@celery_app.task
def process_sped_students_person_sync_webhook(event_data):
    pass

@celery_app.task
def process_plp_course_sync_webhook(event_data):
    """(Original HS Roster Linking Logic) Links courses from the HS Roster to the PLP board."""
    subitem_id = event_data.get('pulseId')
    subitem_board_id = event_data.get('boardId')
    parent_item_id = event_data.get('parentItemId')
    current_value = event_data.get('value')

    linked_courses = monday.get_linked_ids_from_connect_column_value(current_value)
    if not linked_courses: return
    all_courses_item_id = list(linked_courses)[0]

    subject_area_col = monday.get_column_value(subitem_id, subitem_board_id, "dropdown_mks6zjqh")
    subject_area = subject_area_col.get('text') if subject_area_col else None
    if not subject_area: return

    plp_item_ids = monday.get_linked_items_from_board_relation(parent_item_id, HS_COURSE_ROSTER_BOARD_ID, "board_relation_mks270k0")
    if not plp_item_ids: return
    plp_item_id = list(plp_item_ids)[0]
    
    subject_to_plp_column_map = {
        "Math": "board_relation_mkqnbtaf", "ELA": "board_relation_mkqnxyjd",
        "ACE": "board_relation_mkqn34pg", "Other": "board_relation_mkr54dtg"
    }
    plp_target_column_id = subject_to_plp_column_map.get(subject_area)
    if not plp_target_column_id: return

    monday.update_connect_board_column(plp_item_id, PLP_BOARD_ID, plp_target_column_id, all_courses_item_id, "add")
    return True

@celery_app.task
def cleanup_hs_roster_links():
    """Iterates through all HS Roster items and ensures PLP links are correct."""
    pass

@celery_app.task
def process_canvas_sync_webhook(event_data):
    """Handles syncing enrollments and logs results to subitems with user and date."""
    plp_item_id = event_data.get('pulseId')
    trigger_column_id = event_data.get('columnId')
    user_id = event_data.get('userId')

    if not (master_student_ids := monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)):
        return
    master_student_id = list(master_student_ids)[0]
    
    student_name = monday.get_item_name(plp_item_id, PLP_BOARD_ID)
    student_email = (monday.get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_EMAIL_COLUMN) or {}).get('text')
    student_ssid = (monday.get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_SSID_COLUMN) or {}).get('text') or f"monday_{plp_item_id}"

    if not all([student_email, student_name]):
        monday.create_subitem(plp_item_id, "Canvas Sync Failed: Could not find linked student name or email.")
        return
        
    student_details = {"name": student_name, "email": student_email, "ssid": student_ssid}
    
    changer_name = monday.get_user_name(user_id) or "Automation"
    pacific_tz = pytz.timezone('America/Los_Angeles')
    current_date = datetime.now(pacific_tz).strftime('%Y-%m-%d')
    user_log_text = f" on {current_date} by {changer_name}"

    plp_connect_cols = [col.strip() for col in PLP_ALL_CLASSES_CONNECT_COLUMNS_STR.split(',')]
    linked_class_ids, unlinked_class_ids = set(), set()
    
    if trigger_column_id in plp_connect_cols:
        linked_class_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
        unlinked_class_ids = monday.get_linked_ids_from_connect_column_value(event_data.get('previousValue')) - linked_class_ids
    elif trigger_column_id == PLP_CANVAS_SYNC_STATUS_COLUMN_ID:
        for col_id in plp_connect_cols:
            linked_class_ids.update(monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, col_id))

    for class_item_id in linked_class_ids:
        class_item_name = monday.get_item_name(class_item_id, ALL_CLASSES_BOARD_ID) or f"Class Item {class_item_id}"
        subitem_info = monday.create_subitem(plp_item_id, f"Canvas Enrollment: {class_item_name}{user_log_text}")
        
        enrollment_result = None
        if canvas_class_link_ids := monday.get_linked_items_from_board_relation(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN):
            canvas_class_item_id = list(canvas_class_link_ids)[0]
            canvas_course_id = (monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_ID_COLUMN) or {}).get('text')

            if not canvas_course_id or not str(canvas_course_id).strip():
                title_val = monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_TITLE_COLUMN)
                course_title = title_val.get('text') if title_val and title_val.get('text') else None
                if course_title and course_title.strip() and CANVAS_TERM_ID:
                    if new_course := canvas.create_canvas_course(course_title, CANVAS_TERM_ID):
                        canvas_course_id = str(new_course.id)
                        monday.change_column_value_generic(CANVAS_CLASSES_BOARD_ID, canvas_class_item_id, CANVAS_COURSE_ID_COLUMN, canvas_course_id)
            
            if canvas_course_id and str(canvas_course_id).strip():
                sections = set()
                ag_grad_val = monday.get_column_value(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_AG_GRAD_COLUMN)
                ag_grad_text = ag_grad_val.get('text', '') if ag_grad_val else ''
                if "AG" in ag_grad_text: sections.add("A-G")
                if "Grad" in ag_grad_text: sections.add("Grad")
                if (monday.get_column_value(plp_item_id, PLP_BOARD_ID, PLP_OP2_SECTION_COLUMN) or {}).get('text') == "Op2 Section": sections.add("Op2")
                
                for section_name in sections:
                    if section := canvas.create_section_if_not_exists(str(canvas_course_id), section_name):
                        enrollment_result = canvas.enroll_or_create_and_enroll(str(canvas_course_id), section.id, student_details)

        if subitem_info:
            # --- MODIFIED: Updated Failure Message ---
            status_message = "Successfully enrolled in Canvas" if enrollment_result else "NOT SUCCESSFULLY enrolled in Canvas."
            monday.update_long_text_column(subitem_info['board_id'], subitem_info['id'], "long_text8__1", status_message)

    for class_item_id in unlinked_class_ids:
        class_item_name = monday.get_item_name(class_item_id, ALL_CLASSES_BOARD_ID) or f"Class Item {class_item_id}"
        subitem_info = monday.create_subitem(plp_item_id, f"Canvas Unenrollment: {class_item_name}{user_log_text}")
        
        unenroll_result = False
        if canvas_class_link_ids := monday.get_linked_items_from_board_relation(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN):
            canvas_class_item_id = list(canvas_class_link_ids)[0]
            if canvas_course_id := (monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_ID_COLUMN) or {}).get('text'):
                if str(canvas_course_id).strip():
                    unenroll_result = canvas.unenroll_student_from_course(str(canvas_course_id), student_details)
                    
        if subitem_info:
            # --- MODIFIED: Updated Failure Message ---
            status_message = "Successfully unenrolled from Canvas" if unenroll_result else "NOT SUCCESSFULLY unenrolled from Canvas."
            monday.update_long_text_column(subitem_info['board_id'], subitem_info['id'], "long_text8__1", status_message)

    return True
