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
    """Handles generic subitem logging for non-people columns."""
    item_id, user_id = event_data.get('pulseId'), event_data.get('userId')
    current_value, previous_value = event_data.get('value'), event_data.get('previousValue')
    log_type, params = config_rule.get("log_type"), config_rule.get("params", {})
    
    pacific_tz = pytz.timezone('America/Los_Angeles')
    current_date = datetime.now(pacific_tz).strftime('%Y-%m-%d')
    changer_name = monday.get_user_name(user_id) or ("automation" if user_id == -4 else "")
    user_log_text = f" by {changer_name}" if changer_name else ""

    if log_type == "ConnectBoardChange":
        board_id = params.get('linked_board_id')
        prefix_add, prefix_remove = params.get('subitem_name_prefix_add', 'Added'), params.get('subitem_name_prefix_remove', 'Removed')
        current_ids = monday.get_linked_ids_from_connect_column_value(current_value)
        previous_ids = monday.get_linked_ids_from_connect_column_value(previous_value)
        
        for item_id_linked in (current_ids - previous_ids):
            if linked_item_name := monday.get_item_name(item_id_linked, board_id):
                monday.create_subitem(item_id, f"{prefix_add} '{linked_item_name}' on {current_date}{user_log_text}")
        for item_id_linked in (previous_ids - current_ids):
            if linked_item_name := monday.get_item_name(item_id_linked, board_id):
                monday.create_subitem(item_id, f"{prefix_remove} '{linked_item_name}' on {current_date}{user_log_text}")
    return True

@celery_app.task
def process_master_student_person_sync_webhook(event_data):
    """Handles syncing people columns from the Master Student List and logs to subitems."""
    pass

@celery_app.task
def process_sped_students_person_sync_webhook(event_data):
    """Syncs people columns from the SPED Students board."""
    pass

@celery_app.task
def process_hs_roster_linking_webhook(event_data):
    """Links courses from the HS Roster to the PLP board based on subject area."""
    print("INFO: HS_ROSTER_LINKING - Task started.")
    subitem_id = event_data.get('pulseId')
    main_item_id = event_data.get('parentItemId')
    
    linked_courses = monday.get_linked_ids_from_connect_column_value(event_data.get('value'))
    if not linked_courses:
        return print("INFO: HS_ROSTER_LINKING - No course was linked.")
    all_courses_item_id = list(linked_courses)[0]

    subject_area_col = monday.get_column_value(subitem_id, HS_COURSE_ROSTER_BOARD_ID, "dropdown_mks6zjqh")
    subject_area = subject_area_col.get('text') if subject_area_col else None
    if not subject_area:
        return print(f"ERROR: HS_ROSTER_LINKING - Could not determine subject area for subitem {subitem_id}.")

    plp_item_ids = monday.get_linked_items_from_board_relation(main_item_id, HS_COURSE_ROSTER_BOARD_ID, "board_relation_mks270k0")
    if not plp_item_ids:
        return print(f"ERROR: HS_ROSTER_LINKING - Main item {main_item_id} is not linked to a PLP item.")
    plp_item_id = list(plp_item_ids)[0]
    
    subject_to_plp_column_map = {
        "Math": "board_relation_mkqnbtaf",
        "ELA": "board_relation_mkqnxyjd",
        "ACE": "board_relation_mkqn34pg",
        "Other": "board_relation_mkr54dtg"
    }
    
    plp_source_column_id = subject_to_plp_column_map.get(subject_area)
    if not plp_source_column_id:
        return print(f"ERROR: HS_ROSTER_LINKING - Subject area '{subject_area}' has no valid mapping.")

    items_to_link = monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, plp_source_column_id)
    if not items_to_link:
        return print(f"INFO: HS_ROSTER_LINKING - No items to link from PLP column '{plp_source_column_id}'.")

    for item_id_to_link in items_to_link:
         monday.update_connect_board_column(
            item_id=all_courses_item_id,
            board_id=ALL_CLASSES_BOARD_ID,
            connect_column_id="board_relation_mkr5s40q",
            item_to_link_id=item_id_to_link,
            action="add"
        )
    print("INFO: HS_ROSTER_LINKING - Task finished.")
    return True

@celery_app.task
def cleanup_hs_roster_links():
    """Iterates through all HS Roster items and ensures PLP links are correct."""
    print("INFO: CLEANUP_HS_ROSTER - Task started.")
    all_roster_items = monday.get_all_items_with_subitems(
        board_id=int(HS_COURSE_ROSTER_BOARD_ID),
        main_item_column_ids=["board_relation_mks270k0"],
        subitem_column_ids=["board_relation_mkr0bwsf", "dropdown_mks6zjqh"]
    )
    if not all_roster_items: 
        print("INFO: CLEANUP_HS_ROSTER - No items found on the roster board.")
        return True

    subject_map = {
        "Math": "board_relation_mkqnbtaf", "ELA": "board_relation_mkqnxyjd",
        "ACE": "board_relation_mkqn34pg", "Other": "board_relation_mkr54dtg"
    }

    for main_item in all_roster_items:
        plp_col = next((cv for cv in main_item['column_values'] if cv['id'] == 'board_relation_mks270k0'), None)
        plp_ids = monday.get_linked_ids_from_connect_column_value(json.loads(plp_col['value'])) if plp_col and plp_col['value'] else set()
        if not plp_ids: continue
        plp_item_id = list(plp_ids)[0]

        for subitem in main_item.get('subitems', []):
            course_col = next((cv for cv in subitem['column_values'] if cv['id'] == 'board_relation_mkr0bwsf'), None)
            subject_col = next((cv for cv in subitem['column_values'] if cv['id'] == 'dropdown_mks6zjqh'), None)
            
            course_ids = monday.get_linked_ids_from_connect_column_value(json.loads(course_col['value'])) if course_col and course_col['value'] else set()
            subject = subject_col.get('text') if subject_col else None
            
            if not course_ids or not subject: continue
            course_item_id = list(course_ids)[0]
            
            plp_source_col = subject_map.get(subject)
            if not plp_source_col: continue

            expected_links = monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, plp_source_col)
            current_links = monday.get_linked_items_from_board_relation(course_item_id, ALL_CLASSES_BOARD_ID, "board_relation_mkr5s40q")
            
            for link_id in (expected_links - current_links):
                print(f"INFO: CLEANUP - Adding missing link {link_id} to item {course_item_id}")
                monday.update_connect_board_column(course_item_id, ALL_CLASSES_BOARD_ID, "board_relation_mkr5s40q", link_id, "add")
    
    print("INFO: CLEANUP_HS_ROSTER - Task finished.")
    return True

@celery_app.task
def process_canvas_sync_webhook(event_data):
    """Handles syncing enrollments and logs results to subitems with user and date."""
    print("INFO: CANVAS_SYNC - Task started.")
    plp_item_id = event_data.get('pulseId')
    trigger_column_id = event_data.get('columnId')
    user_id = event_data.get('userId')

    if not (master_student_ids := monday.get_linked_items_from_board_relation(plp_item_id, PLP_BOARD_ID, PLP_TO_MASTER_STUDENT_CONNECT_COLUMN)):
        return print(f"ERROR: PLP item {plp_item_id} not linked to a Master Student.")
    master_student_id = list(master_student_ids)[0]
    
    student_name = monday.get_item_name(plp_item_id, PLP_BOARD_ID)
    student_email = (monday.get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_EMAIL_COLUMN) or {}).get('text')
    student_ssid = (monday.get_column_value(master_student_id, MASTER_STUDENT_BOARD_ID, MASTER_STUDENT_SSID_COLUMN) or {}).get('text') or f"monday_{plp_item_id}"

    if not all([student_email, student_name]):
        print(f"ERROR: Student email or name not found for PLP item {plp_item_id}.")
        monday.create_subitem(plp_item_id, "Canvas Sync Failed: Could not find linked student name or email.")
        return
        
    student_details = {"name": student_name, "email": student_email, "ssid": student_ssid}
    print(f"INFO: Syncing for student: {student_details}")

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
        print(f"--- Processing Enrollment for Class ID: {class_item_id} ---")
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
            status_message = "Successfully enrolled in Canvas" if enrollment_result else "Failed to enroll in Canvas."
            monday.update_long_text_column(subitem_info['board_id'], subitem_info['id'], "long_text8__1", status_message)

    for class_item_id in unlinked_class_ids:
        print(f"--- Processing Unenrollment for Class ID: {class_item_id} ---")
        class_item_name = monday.get_item_name(class_item_id, ALL_CLASSES_BOARD_ID) or f"Class Item {class_item_id}"
        subitem_info = monday.create_subitem(plp_item_id, f"Canvas Unenrollment: {class_item_name}{user_log_text}")
        
        unenroll_result = False
        if canvas_class_link_ids := monday.get_linked_items_from_board_relation(class_item_id, ALL_CLASSES_BOARD_ID, ALL_CLASSES_CANVAS_CONNECT_COLUMN):
            canvas_class_item_id = list(canvas_class_link_ids)[0]
            if canvas_course_id := (monday.get_column_value(canvas_class_item_id, CANVAS_CLASSES_BOARD_ID, CANVAS_COURSE_ID_COLUMN) or {}).get('text'):
                if str(canvas_course_id).strip():
                    unenroll_result = canvas.unenroll_student_from_course(str(canvas_course_id), student_email)
                    
        if subitem_info:
            status_message = "Successfully unenrolled from Canvas" if unenroll_result else "Failed to unenroll from Canvas."
            monday.update_long_text_column(subitem_info['board_id'], subitem_info['id'], "long_text8__1", status_message)

    print("INFO: CANVAS_SYNC - Task finished.")
    return True
