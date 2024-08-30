import time
from sqlalchemy import func, or_, and_, extract
from fastapi import HTTPException
import itertools
from datetime import datetime

def initialize_parameters(view_type, team_id, user_id, year, month, quarter, estCloseDateStart, estCloseDateEnd, deal_filter, product, min_value, max_value, customer_type, customer_value, project):
    if view_type is None:
        view_type = "all"
    quarter_values = [q.split(',') for q in quarter] if quarter else []
    deal_filter_option = [option.split(',') for option in deal_filter] if deal_filter else []
    product_values = [pd.split(',') for pd in product] if product else []
    return view_type, quarter_values, deal_filter_option, product_values

def get_user_and_company_info(current_user, db):
    user_query = db.query(models.Users).filter(models.Users.id == current_user).first()
    if not user_query:
        raise HTTPException(status_code=400, detail="User not found")
    company_id = user_query.company_id
    role_name = user_query.role.name
    return user_query, company_id, role_name

def get_board_id(board_id, company_id, db):
    if board_id is None:
        board = db.query(models.Boards).filter(models.Boards.company_id == company_id, models.Boards.order_id == 1).first()
        if board:
            board_id = board.id
    return board_id

def get_deal_latest_log(company_id, db):
    deal_latest_log = db.query(func.max(models.Deal_Logs.id)).filter(models.Users.id == models.Deal_Logs.user_id, models.Users.company_id == company_id).group_by(models.Deal_Logs.deal_id).all()
    return [log[0] for log in deal_latest_log]

def get_pipeline_query(company_id, board_id, db):
    return db.query(models.Pipelines).filter(models.Pipelines.company_id == company_id, models.Pipelines.board_id == board_id).all()

def get_team_query(company_id, board_id, db):
    delete_team = [team_log.team_id for team_log in db.query(models.Team_Logs).filter(models.Team_Logs.team_id == models.Teams.id, models.Team_Logs.action == 'delete').all()]
    return db.query(models.Teams).filter(and_(models.Teams.company_id == company_id, models.Teams.board_id == board_id, models.Teams.id.notin_(delete_team))).all()

def validate_user(user_query, current_user):
    if user_query.id != int(current_user):
        raise HTTPException(status_code=400, detail="Unauthorized")
    if user_query.status != "active":
        raise HTTPException(status_code=400, detail="User not active")

def get_role_permissions(current_user, board_id, db):
    return permission_detail(current_user, board_id, db)

def handle_admin_role(team_query, year, board_id, db):
    sale_target_query = db.query(models.Sales_Targets).join(models.Teams, models.Teams.id == models.Sales_Targets.team_id).filter(
        models.Sales_Targets.team_id.in_([team.id for team in team_query]),
        models.Teams.board_id == board_id,
        extract('year', models.Sales_Targets.year) == year
    ).all()
    total_target = sum(target.value for target in sale_target_query)
    return total_target

def handle_user1_role(current_user, role_permissions, board_id, db):
    director_team_id = role_permissions['user1']['director']
    member_team_id = role_permissions['user1']['member']
    is_head_sub_team, head_team_id = check_head_sub_team(current_user, member_team_id, board_id, db)
    # Implement the logic for user1 role based on the extracted information
    # ...
    return total_target

def handle_user2_role():
    return 0

def get_individual_customer_ids(db):
    individual_customer_ids_query = db.query(models.Customers.id).filter(models.Customers.customer_type == "individual").all()
    return list(itertools.chain(*individual_customer_ids_query))

def get_customer_and_contact_latest_log(company_id, db):
    customer_latest_log = db.query(func.max(models.Customer_Logs.id)).filter(models.Users.company_id == company_id, models.Customers.user_id == models.Users.id, models.Customer_Logs.customer_id == models.Customers.id).group_by(models.Customers.id).all()
    contact_latest_log = db.query(func.max(models.Contact_Logs.id)).filter(models.Users.company_id == company_id, models.Contacts.user_id == models.Users.id, models.Contact_Logs.contact_id == models.Contacts.id).group_by(models.Contacts.id).all()
    return list(itertools.chain(*customer_latest_log)), list(itertools.chain(*contact_latest_log))

def calculate_pipeline_data(pipeline_query, base_deal_query, role_name, deal_filter_option, quarter_values, month, estCloseDateStart, estCloseDateEnd, min_value, max_value, project, customer_type, product_values, total_target):
    pipeline_data = []
    remaining_pipeline = 0
    win_value = 0
    lose_value = 0
    all_value = 0
    for pipeline in pipeline_query:
        deal_query = base_deal_query.filter(models.Deal_Logs.pipeline_id == pipeline.id)
        if role_name != 'admin' and role_name != 'superadmin':
            deal_query = deal_query.filter(models.Deal_Logs.user_id == models.Users.id)
        # Apply filters and calculate values
        # ...
        pipeline_data.append({
            # Populate pipeline data
        })
    remaining_target = total_target - win_value
    return pipeline_data, remaining_pipeline, remaining_target

def deal_card(board_id, view_type, team_id, user_id, year, month, quarter, estCloseDateStart, estCloseDateEnd, deal_filter, product, min_value, max_value, customer_type, customer_value, project, current_user, db):
    start_time = time.time()
    
    view_type, quarter_values, deal_filter_option, product_values = initialize_parameters(view_type, team_id, user_id, year, month, quarter, estCloseDateStart, estCloseDateEnd, deal_filter, product, min_value, max_value, customer_type, customer_value, project)
    user_query, company_id, role_name = get_user_and_company_info(current_user, db)
    validate_user(user_query, current_user)
    board_id = get_board_id(board_id, company_id, db)
    deal_latest_log = get_deal_latest_log(company_id, db)
    base_deal_query = db.query(models.Deal_Logs).filter(models.Deal_Logs.id.in_(deal_latest_log), models.Deal_Logs.board_id == board_id, or_(models.Deal_Logs.is_deleted == None, models.Deal_Logs.is_deleted == False))
    pipeline_query = get_pipeline_query(company_id, board_id, db)
    team_query = get_team_query(company_id, board_id, db)
    
    role_permissions = get_role_permissions(current_user, board_id, db)
    total_target = 0
    if "admin" in role_permissions:
        total_target = handle_admin_role(team_query, year, board_id, db)
    elif "user1" in role_permissions:
        total_target = handle_user1_role(current_user, role_permissions, board_id, db)
    elif "user2" in role_permissions:
        total_target = handle_user2_role()
    
    individual_customer_ids = get_individual_customer_ids(db)
    customer_latest_log, contact_latest_log = get_customer_and_contact_latest_log(company_id, db)
    
    pipeline_data, remaining_pipeline, remaining_target = calculate_pipeline_data(pipeline_query, base_deal_query, role_name, deal_filter_option, quarter_values, month, estCloseDateStart, estCloseDateEnd, min_value, max_value, project, customer_type, product_values, total_target)
    
    filter_deal = {
        "remainingPipeline": f'{int(remaining_pipeline):,}',
        "remainingTarget": f'{int(remaining_target):,}',
        "totalTarget": f'{int(total_target):,}',
        "pipelineData": pipeline_data
    }
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Time taken: {elapsed_time:.2f} seconds")
    return filter_deal