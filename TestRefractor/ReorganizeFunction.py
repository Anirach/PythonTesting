#Reduce Database Queries: Minimize the number of database queries by combining them where possible.
#Use Efficient Data Structures: Use dictionaries and sets for faster lookups.
#Batch Processing: Process data in batches to reduce the overhead of multiple queries.
#Avoid Redundant Computations: Cache results of expensive computations if they are used multiple times.


def deal_card(board_id, view_type, team_id, user_id, year, month, quarter, estCloseDateStart, estCloseDateEnd, deal_filter, product, min_value, max_value, customer_type, customer_value, project, current_user, db):
    start_time = time.time()
    
    view_type = view_type or "all"
    print("deal_card")
    print("params: ", view_type, team_id, user_id, year, month, quarter, estCloseDateStart, estCloseDateEnd, deal_filter, product, min_value, max_value, customer_type, customer_value, project)
    
    user_query = db.query(models.Users).filter(models.Users.id == current_user).first()
    if not user_query:
        raise HTTPException(status_code=400, detail="User not found")
    if user_query.id != int(current_user):
        raise HTTPException(status_code=400, detail="Unauthorized")
    if user_query.status != "active":
        raise HTTPException(status_code=400, detail="User not active")
    
    company_id = user_query.company_id
    role_name = user_query.role.name
    
    quarter_values = [q.split(',') for q in quarter] if quarter else []
    deal_filter_option = [option.split(',') for option in deal_filter] if deal_filter else []
    product_values = [pd.split(',') for pd in product] if product else []

    if board_id is None:
        board = db.query(models.Boards).filter(models.Boards.company_id == company_id, models.Boards.order_id == 1).first()
        board_id = board.id if board else None
    
    deal_latest_log = db.query(func.max(models.Deal_Logs.id)).filter(models.Users.id == models.Deal_Logs.user_id, models.Users.company_id == company_id).group_by(models.Deal_Logs.deal_id).all()
    deal_latest_log = [log[0] for log in deal_latest_log]
    base_deal_query = db.query(models.Deal_Logs).filter(
        models.Deal_Logs.id.in_(deal_latest_log),
        models.Deal_Logs.board_id == board_id,
        or_(models.Deal_Logs.is_deleted == None, models.Deal_Logs.is_deleted == False)
    )

    pipeline_query = db.query(models.Pipelines).filter(models.Pipelines.company_id == company_id, models.Pipelines.board_id == board_id).all()
    
    delete_team = {team_log.team_id for team_log in db.query(models.Team_Logs).filter(models.Team_Logs.team_id == models.Teams.id, models.Team_Logs.action == 'delete').all()}
    team_query = db.query(models.Teams).filter(and_(models.Teams.company_id == company_id, models.Teams.board_id == board_id, models.Teams.id.notin_(delete_team))).all()
    if not team_query:
        raise HTTPException(status_code=400, detail="Board not found in user's company")
    
    total_target = 0
    role_permissions = permission_detail(current_user, board_id, db)
    
    if team_id is None and user_id is None:
        if "admin" in role_permissions:
            sale_target_query = db.query(models.Sales_Targets).join(models.Teams, models.Teams.id == models.Sales_Targets.team_id).filter(
                models.Sales_Targets.team_id.in_([team.id for team in team_query]),
                models.Teams.board_id == board_id,
                extract('year', models.Sales_Targets.year) == year
            ).all()
            total_target = sum(target.value for target in sale_target_query)
        elif "user1" in role_permissions:
            director_team_id = role_permissions['user1']['director']
            member_team_id = role_permissions['user1']['member']
            is_head_sub_team, head_team_id = check_head_sub_team(current_user, member_team_id, board_id, db)
            # Implement the logic for user1 role based on the extracted information
            # ...
        elif "user2" in role_permissions:
            total_target = 0
    elif team_id and user_id:
        if "admin" in role_permissions:
            team_id = int(team_id)
            user_id = int(user_id)
            user_members = user_team_child(user_id, team_id, db)
            user_members.append(user_id)
            sale_target_query = db.query(models.Sales_Targets).join(models.Teams, models.Teams.id == models.Sales_Targets.team_id).filter(
                models.Sales_Targets.team_id == team_id,
                models.Sales_Targets.user_id.in_(user_members),
                models.Teams.board_id == board_id,
                extract('year', models.Sales_Targets.year) == year
            ).all()
            total_target = sum(target.value for target in sale_target_query)
        elif "user1" in role_permissions:
            director_team_id = role_permissions['user1']['director']
            member_team_id = role_permissions['user1']['member']
            is_head_sub_team, head_team_id = check_head_sub_team(current_user, member_team_id, board_id, db)
            team_id = int(team_id)
            user_id = int(user_id)
            user_members = user_team_child(user_id, team_id, db)
            user_members.append(user_id)
            sale_target_query = db.query(models.Sales_Targets).join(models.Teams, models.Teams.id == models.Sales_Targets.team_id).filter(
                models.Sales_Targets.team_id == team_id,
                models.Sales_Targets.user_id.in_(user_members),
                models.Teams.board_id == board_id,
                extract('year', models.Sales_Targets.year) == year
            ).all()
            total_target = sum(target.value for target in sale_target_query)
        elif "user2" in role_permissions:
            total_target = 0
    elif team_id:
        if "admin" in role_permissions:
            sale_target_query = db.query(models.Sales_Targets).join(models.Teams, models.Teams.id == models.Sales_Targets.team_id).filter(
                models.Sales_Targets.team_id == int(team_id),
                models.Teams.board_id == board_id,
                extract('year', models.Sales_Targets.year) == year
            ).all()
            total_target = sum(target.value for target in sale_target_query)
        elif "user1" in role_permissions:
            director_team_id = role_permissions['user1']['director']
            member_team_id = role_permissions['user1']['member']
            is_head_sub_team, head_team_id = check_head_sub_team(current_user, member_team_id, board_id, db)
            team_id = int(team_id)
            if team_id in director_team_id:
                # Implement the logic for user1 role based on the extracted information
                # ...
            elif team_id in member_team_id and team_id not in head_team_id:
                # Implement the logic for user1 role based on the extracted information
                # ...
            elif team_id in head_team_id and is_head_sub_team:
                # Implement the logic for user1 role based on the extracted information
                # ...
        elif "user2" in role_permissions:
            total_target = 0
    elif user_id:
        if "admin" in role_permissions:
            sale_target_query = db.query(models.Sales_Targets).join(models.Teams, models.Teams.id == models.Sales_Targets.team_id).filter(
                models.Sales_Targets.user_id == int(user_id),
                models.Teams.board_id == board_id,
                extract('year', models.Sales_Targets.year) == year
            ).all()
            total_target = sum(target.value for target in sale_target_query)
        elif "user1" in role_permissions:
            director_team_id = role_permissions['user1']['director']
            member_team_id = role_permissions['user1']['member']
            is_head_sub_team, head_team_id = check_head_sub_team(current_user, member_team_id, board_id, db)
            if int(user_id) == current_user:
                # Implement the logic for user1 role based on the extracted information
                # ...
            elif any(int(user_id) in team_info['director_member'] for team_info in director_member):
                # Implement the logic for user1 role based on the extracted information
                # ...
            elif any(int(user_id) in team_info['sub_team_member'] for team_info in sub_team_member):
                # Implement the logic for user1 role based on the extracted information
                # ...
        elif "user2" in role_permissions:
            total_target = 0

    individual_customer_ids = {id for id, in db.query(models.Customers.id).filter(models.Customers.customer_type == "individual").all()}
    customer_latest_log = {id for id, in db.query(func.max(models.Customer_Logs.id)).filter(models.Users.company_id == company_id, models.Customers.user_id == models.Users.id, models.Customer_Logs.customer_id == models.Customers.id).group_by(models.Customers.id).all()}
    contact_latest_log = {id for id, in db.query(func.max(models.Contact_Logs.id)).filter(models.Users.company_id == company_id, models.Contacts.user_id == models.Users.id, models.Contact_Logs.contact_id == models.Contacts.id).group_by(models.Contacts.id).all()}

    pipeline_data = []
    remaining_pipeline = 0
    win_value = 0
    lose_value = 0
    all_value = 0

    if view_type == "all":
        for pipeline in pipeline_query:
            print("pipeline name:", pipeline.name)
            deal_query = base_deal_query.filter(models.Deal_Logs.pipeline_id == pipeline.id)
            if role_name not in {'admin', 'superadmin'}:
                deal_query = deal_query.filter(models.Deal_Logs.user_id == models.Users.id)
            if deal_filter_option:
                # Apply deal filter options
                # ...
            if quarter_values:
                # Apply quarter filter
                # ...
            if month:
                # Apply month filter
                # ...
            if estCloseDateStart:
                # Apply estCloseDateStart filter
                # ...
            if estCloseDateEnd:
                # Apply estCloseDateEnd filter
                # ...
            if min_value:
                # Apply min_value filter
                # ...
            if max_value:
                # Apply max_value filter
                # ...
            if project:
                # Apply project filter
                # ...
            if customer_type == 'individual':
                # Apply individual customer filter
                # ...
            elif customer_type == 'company':
                # Apply company customer filter
                # ...
            if product_values:
                # Apply product filter
                # ...

            pipeline_deals = deal_query.all()
            total_deal = len(pipeline_deals)
            total_value = 0
            un_select_pipeline = 0

            for deal in pipeline_deals:
                is_deal_in_focus = db.query(models.Deal_Focus).filter(models.Deal_Focus.deal_id == deal.deal_id, models.Deal_Focus.user_id == current_user).first() is not None
                user = db.query(models.Users).filter(models.Users.id == deal.user_id).first()
                lose_type = db.query(models.Lose_Types).filter(models.Lose_Types.company_id == company_id, models.Lose_Types.id == deal.lose_type_id).first()
                lose_type_name = lose_type.name if lose_type and lose_type.name else ''
                customer_type_check = db.query(models.Customers.customer_type).join(models.Contacts, models.Customers.id == models.Contacts.customer_id).filter(models.Contacts.id == deal.contact_id).scalar()
                if customer_type_check == "company":
                    customer_query = db.query(models.Customers, models.Customer_Logs).join(models.Contacts, models.Contacts.customer_id == models.Customers.id).filter(models.Contacts.id == deal.contact_id).first()
                # Process deal data
                # ...

            remaining_pipeline += total_value
            current_date = datetime.now().date()
            deal_data = []
            user_data_for_pipeline = []

            user_data_query = db.query(
                models.Users.id.label("userId"),
                models.Users.name.label("name"),
                models.Users.photo.label("photo"),
                func.count(models.Deal_Logs.deal_id).label("totalDeal"),
            ).join(models.Deal_Logs, models.Deal_Logs.user_id == models.Users.id).filter(models.Deal_Logs.pipeline_id == pipeline.id).group_by(models.Users.id, models.Users.name)
            user_data_list = user_data_query.all()

            for deal in pipeline_deals:
                # Process deal data
                # ...

            if pipeline.probability in {100, 0}:
                # Process won/lose pipeline
                # ...
            else:
                # Process other pipeline
                # ...

            remaining_target = total_target - win_value

        print("return data")
        filter_deal = {
            "remainingPipeline": f'{int(remaining_pipeline - win_value - lose_value - all_value):,}',
            "remainingTarget": f'{int(remaining_target):,}',
            "totalTarget": f'{int(total_target):,}',
            "displayDealHealth": str(True) if display_deal_health is None else str(display_deal_health.display_deal_health),
            "pipelineData": pipeline_data
        }
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Time taken: {elapsed_time:.2f} seconds")
        return filter_deal
    else:
        remaining_target = total_target - win_value
        win_lose = win_value + lose_value
        filter_deal = {
            "remainingPipeline": f'{int(remaining_pipeline - win_lose):,}',
            "remainingTarget": f'{int(remaining_target):,}',
            "totalTarget": f'{int(total_target):,}',
            "pipelineData": []
        }
        return filter_deal