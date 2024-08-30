def deal_card(board_id, view_type, team_id , user_id, year, month, quarter, estCloseDateStart, estCloseDateEnd, deal_filter, product, min_value, max_value,customer_type,customer_value,project, current_user, db):
    start_time = time.time()
    
    if view_type is None:
        view_type = "all"
    print("deal_card")
    print("params: ",view_type, team_id, user_id, year, month, quarter, estCloseDateStart, estCloseDateEnd, deal_filter, product, min_value, max_value,customer_type,customer_value,project)
    user_query = db.query(models.Users).filter(models.Users.id == current_user).first()
    company_id = user_query.company_id
    role_name = user_query.role.name
    
    # convert quarter, deal_filter, product to list
    quarter_values = []
    if quarter:
        for q in quarter:
            quarter_value = q.split(',')
            for qt in quarter_value:
                if qt.strip():
                    quarter_values.append(int(qt))
    deal_filter_option = []
    if deal_filter:
        for option in deal_filter:
            deal_filter_options = option.split(',')
            for deal_option in deal_filter_options:
                if deal_option.strip():
                    deal_filter_option.append(deal_option)
    product_values = []
    if product:
        for pd in product:
            pd_value = pd.split(',')
            for product_list in pd_value:
                if product_list.strip():
                    product_values.append(int(product_list))

    # board order
    if board_id is None:
        board = db.query(models.Boards).filter(models.Boards.company_id == company_id, models.Boards.order_id == 1).first()
        if board:
            board_id = board.id
    
    # deal log lastest
    deal_latest_log = db.query(func.max(models.Deal_Logs.id)).filter(models.Users.id==models.Deal_Logs.user_id, models.Users.company_id==company_id).group_by(models.Deal_Logs.deal_id).all()
    deal_latest_log = [log[0] for log in deal_latest_log]
    base_deal_query = db.query(models.Deal_Logs).filter(
        models.Deal_Logs.id.in_(deal_latest_log),
        models.Deal_Logs.board_id == board_id,
        or_(models.Deal_Logs.is_deleted == None, models.Deal_Logs.is_deleted == False)
    )

    # pipeline query
    pipeline_query = db.query(models.Pipelines).filter(models.Pipelines.company_id == company_id,models.Pipelines.board_id == board_id).all()
    
    # team query    
    delete_team = []
    team_log_query = (db.query(models.Team_Logs).filter(models.Team_Logs.team_id == models.Teams.id).filter(models.Team_Logs.action == 'delete').all())
    for team_log in team_log_query:
        delete_team.append(team_log.team_id)
    team_query = db.query(models.Teams).filter(and_(models.Teams.company_id == company_id, models.Teams.board_id == board_id, models.Teams.id.notin_(delete_team))).all()  

    if not user_query:
        raise HTTPException(status_code=400, detail="User not found")

    if user_query.id != int(current_user):
        raise HTTPException(status_code=400, detail="Unauthorized")
    
    if user_query.status != "active":
        raise HTTPException(status_code=400, detail="User not active")
    
    if not team_query:
        raise HTTPException(status_code=400, detail="Board not found in user's company")
    
    # sale target
    total_target = 0
    role_permissions = permission_detail(current_user, board_id, db)
    if team_id is None and user_id is None:
        if "admin" in role_permissions:
            
            sale_target_query = (
                db.query(models.Sales_Targets)
                .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                .filter(
                    models.Sales_Targets.team_id.in_([team.id for team in team_query]),
                    models.Teams.board_id == board_id,
                    extract('year', models.Sales_Targets.year) == year
                )
                .all()
            )

            for target in sale_target_query:
                total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                # print("total_target_sub_member",total_target_sub_member)
        
        elif "user1" in role_permissions:

            # print("user1")

            sub_team_member = []

            director_team_id = role_permissions['user1']['director']
            member_team_id = role_permissions['user1']['member']

            is_head_sub_team, head_team_id = check_head_sub_team(current_user, member_team_id, board_id, db)
            
            # print("director_team_id",director_team_id)
            # print("member_team_id",member_team_id)
            # print("is_head_sub_team",is_head_sub_team)
            # print("head_team_id",head_team_id)
            
            # be director / member / head sub team
            if len(director_team_id) > 0 and len(member_team_id) > 0 and is_head_sub_team == True:
                # print("director and member and head")
                total_target_director = 0
                total_target_member = 0
                total_target_head = 0
                total_target_sub_member = 0

                # sale target director
                for team in director_team_id:
                    sale_target_query = (
                        db.query(models.Sales_Targets)
                        .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                        .filter(
                            models.Sales_Targets.team_id == int(team),
                            models.Teams.board_id == board_id,
                            extract('year', models.Sales_Targets.year) == year
                        )
                        .all()
                    )

                    for target in sale_target_query:
                        total_target_director += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))

                # sale target member
                for team in member_team_id:
                    if team not in head_team_id:
                        sale_target_query = db.query(models.Sales_Targets).filter(
                            models.Sales_Targets.team_id == int(team),
                            models.Sales_Targets.user_id == current_user,
                            extract('year', models.Sales_Targets.year) == year
                        ).all() 

                        for target in sale_target_query:
                            total_target_member += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))

                # sale target head
                for team in head_team_id:
                    user_members = user_team_child(current_user, team, db)
                    sub_team_member.append({'head_team_id': team, 'sub_team_member': user_members})

                # print("sub_team_members_list", sub_team_member)
                
                for team_info in sub_team_member:
                    head_team_id = team_info['head_team_id']
                    sub_team_members = team_info['sub_team_member']

                    sale_target_query = db.query(models.Sales_Targets).filter(
                        models.Sales_Targets.team_id == head_team_id,
                        models.Sales_Targets.user_id.in_(sub_team_members),
                        extract('year', models.Sales_Targets.year) == year
                    ).all() 

                    head_target_query = db.query(models.Sales_Targets).filter(
                        models.Sales_Targets.team_id == head_team_id,
                        models.Sales_Targets.user_id == current_user,
                        extract('year', models.Sales_Targets.year) == year
                    ).all() 

                    for target in sale_target_query:
                        total_target_sub_member += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                        # print("total_target_sub_member",total_target_sub_member)
                    
                    for target in head_target_query:
                        total_target_head += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                        # print("total_target_head", total_target_head)
            
                # sum total target
                # print("----------------------------------------------")
                # print("total_target_director",total_target_director)
                # print("total_target_member",total_target_member)
                # print("total_target_head",total_target_head)
                # print("total_target_sub_member",total_target_sub_member)
                total_target = total_target_director + total_target_member + total_target_head + total_target_sub_member
                # print("----------------------------------------------")
                # print("Total Target:", total_target)

            # be director / member
            elif len(director_team_id) > 0 and len(member_team_id) > 0 and is_head_sub_team == False:
                # print("director and member")
                total_target_director = 0
                total_target_member = 0

                # sale target director
                for team in director_team_id:
                    sale_target_query = (
                        db.query(models.Sales_Targets)
                        .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                        .filter(
                            models.Sales_Targets.team_id == int(team),
                            models.Teams.board_id == board_id,
                            extract('year', models.Sales_Targets.year) == year
                        )
                        .all()
                    )

                    for target in sale_target_query:
                        total_target_director += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))

                # sale target member
                for team in member_team_id:
                    sale_target_query = db.query(models.Sales_Targets).filter(
                        models.Sales_Targets.team_id == int(team),
                        models.Sales_Targets.user_id == current_user,
                        extract('year', models.Sales_Targets.year) == year
                    ).all() 

                    for target in sale_target_query:
                        total_target_member += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))

                # sum total target
                total_target = total_target_director + total_target_member
                # print("Total Target:", total_target)

            # be director / head sub team
            elif len(director_team_id) > 0 and is_head_sub_team == True:
                # print("director and head")
                total_target_director = 0
                total_target_head = 0
                total_target_sub_member = 0

                # sale target director
                for team in director_team_id:
                    sale_target_query = (
                        db.query(models.Sales_Targets)
                            .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                            .filter(
                                models.Sales_Targets.team_id == int(team),
                                models.Teams.board_id == board_id,
                                extract('year', models.Sales_Targets.year) == year
                            )
                            .all()
                    )

                    for target in sale_target_query:
                        total_target_director += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))

                # sale target head
                for team in head_team_id:
                    user_members = user_team_child(current_user, team, db)
                    sub_team_member.append({'head_team_id': team, 'sub_team_member': user_members})

                # print("sub_team_members_list", sub_team_member)
                
                for team_info in sub_team_member:
                    head_team_id = team_info['head_team_id']
                    sub_team_members = team_info['sub_team_member']

                    sale_target_query = (
                        db.query(models.Sales_Targets)
                        .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                        .filter(
                        models.Sales_Targets.team_id == head_team_id,
                        models.Sales_Targets.user_id.in_(sub_team_members),
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    ).all())

                    head_target_query = (
                        db.query(models.Sales_Targets)
                        .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                        .filter(
                        models.Sales_Targets.team_id == head_team_id,
                        models.Sales_Targets.user_id == current_user,
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    ).all())

                    for target in sale_target_query:
                        total_target_sub_member += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                        # print("total_target_sub_member",total_target_sub_member)
                    
                    for target in head_target_query:
                        total_target_head += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                        # print("total_target_head", total_target_head)
            
                # sum total target
                # print("total_target_director",total_target_director)
                # print("total_target_head",total_target_head)
                # print("total_target_sub_member",total_target_sub_member)
                total_target = total_target_director + total_target_head + total_target_sub_member
                # print("Total Target:", total_target)

            # be member / head sub team
            elif len(member_team_id) > 0 and is_head_sub_team == True:
                # print("member and head")
                total_target_member = 0
                total_target_head = 0
                total_target_sub_member = 0

                # sale target member
                for team in member_team_id:
                    if team not in head_team_id:
                        sale_target_query = (
                            db.query(models.Sales_Targets)
                            .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                            .filter(
                            models.Sales_Targets.team_id == int(team),
                            models.Sales_Targets.user_id == current_user,
                            models.Teams.board_id == board_id,
                            extract('year', models.Sales_Targets.year) == year
                        ).all())

                        for target in sale_target_query:
                            total_target_member += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                            # print("total_target_member",total_target_member)

                # sale target head
                for team in head_team_id:
                    user_members = user_team_child(current_user, team, db)
                    sub_team_member.append({'head_team_id': team, 'sub_team_member': user_members})

                # print("sub_team_members_list", sub_team_member)
                
                for team_info in sub_team_member:
                    head_team_id = team_info['head_team_id']
                    sub_team_members = team_info['sub_team_member']

                    sale_target_query = (
                        db.query(models.Sales_Targets)
                        .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                        .filter(
                        models.Sales_Targets.team_id == head_team_id,
                        models.Sales_Targets.user_id.in_(sub_team_members),
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    ).all()) 

                    head_target_query = (
                        db.query(models.Sales_Targets)
                        .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                        .filter(
                        models.Sales_Targets.team_id == head_team_id,
                        models.Sales_Targets.user_id == current_user,
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    ).all()) 

                    for target in sale_target_query:
                        total_target_sub_member += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                        # print("total_target_head",total_target_sub_member)
                    
                    for target in head_target_query:
                        total_target_head += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                        # print("total_target_head", total_target_head)
            
                # sum total target
                # print("total_target_member",total_target_member)
                # print("total_target_head",total_target_head)
                # print("total_target_sub_member",total_target_sub_member)
                total_target = total_target_member + total_target_head + total_target_sub_member
                # print("Total Target:", total_target)

            # be only director    
            elif len(director_team_id) > 0:
                # print("director")
                for team in director_team_id:
                    sale_target_query = (
                        db.query(models.Sales_Targets)
                        .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                        .filter(
                        models.Sales_Targets.team_id == int(team),
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    ).all()) 

                    for target in sale_target_query:
                        total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
            
            # be only member
            elif len(member_team_id) > 0 and is_head_sub_team == False:
                # print("member")
                for team in member_team_id:
                    
                    sale_target_query = (
                        db.query(models.Sales_Targets)
                        .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                        .filter(
                        models.Sales_Targets.team_id == int(team),
                        models.Sales_Targets.user_id == current_user,
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    ).all()) 

                    for target in sale_target_query:
                        total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                        # print("total_target",total_target)
        
            # be only head sub team
            elif is_head_sub_team == True:
                # print("head")
                total_target_head = 0
                total_target_sub_member = 0
                
                # sale target head
                for team in head_team_id:
                    user_members = user_team_child(current_user, team, db)
                    sub_team_member.append({'head_team_id': team, 'sub_team_member': user_members})

                # print("sub_team_members_list", sub_team_member)
                
                for team_info in sub_team_member:
                    head_team_id = team_info['head_team_id']
                    sub_team_members = team_info['sub_team_member']

                    sale_target_query = (
                        db.query(models.Sales_Targets)
                        .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                        .filter(
                        models.Sales_Targets.team_id == head_team_id,
                        models.Sales_Targets.user_id.in_(sub_team_members),
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    ).all()) 

                    head_target_query = (
                        db.query(models.Sales_Targets)
                        .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                        .filter(
                        models.Sales_Targets.team_id == head_team_id,
                        models.Sales_Targets.user_id == current_user,
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    ).all()) 

                    for target in sale_target_query:
                        total_target_sub_member += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                        # print("total_target_head",total_target_sub_member)
                    
                    for target in head_target_query:
                        total_target_head += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                        # print("total_target_head", total_target_head)
            
                # sum total target
                # print("total_target_head",total_target_head)
                # print("total_target_sub_member",total_target_sub_member)
                total_target = total_target_head + total_target_sub_member
                # print("Total Target:", total_target)
        
        elif "user2" in role_permissions:
            total_target = 0

    elif team_id and user_id:
        if "admin" in role_permissions:
            team_id = int(team_id)
            user_id = int(user_id)
            
            user_members = user_team_child(user_id, team_id, db)
            user_members.append(user_id)
                
            sale_target_query = (
                db.query(models.Sales_Targets)
                .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                .filter(
                    models.Sales_Targets.team_id == team_id,
                    models.Sales_Targets.user_id.in_(user_members),
                    models.Teams.board_id == board_id,
                    extract('year', models.Sales_Targets.year) == year
                ).all()
            ) 
            for target in sale_target_query:
                total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))

        elif "user1" in role_permissions:
            
            director_member = []

            director_team_id = role_permissions['user1']['director']
            member_team_id = role_permissions['user1']['member']

            is_head_sub_team, head_team_id = check_head_sub_team(current_user, member_team_id, board_id, db)

            team_id = int(team_id)
            user_id = int(user_id)
            
            user_members = user_team_child(user_id, team_id, db)
            user_members.append(user_id)
            director_member.append({'director_team_id': team_id, 'director_member': user_members})

                
            # if team_id in director_team_id:

            sale_target_query = (
                db.query(models.Sales_Targets)
                .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                .filter(
                    models.Sales_Targets.team_id == team_id,
                    models.Sales_Targets.user_id.in_(user_members),
                    models.Teams.board_id == board_id,
                    extract('year', models.Sales_Targets.year) == year
                ).all()
            ) 

            for target in sale_target_query:
                total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))   
               
    elif team_id:
        
        if "admin" in role_permissions:
            # print("-----admin")
            
            sale_target_query = (
                db.query(models.Sales_Targets)
                .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                .filter(
                    models.Sales_Targets.team_id == int(team_id),
                    models.Teams.board_id == board_id,
                    extract('year', models.Sales_Targets.year) == year
                )
                .all()
            )

            for target in sale_target_query:
                total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                # print("total_target",total_target)
        
        elif "user1" in role_permissions:

            # print("-----user1")

            sub_team_member = []

            director_team_id = role_permissions['user1']['director']
            member_team_id = role_permissions['user1']['member']

            is_head_sub_team, head_team_id = check_head_sub_team(current_user, member_team_id, board_id, db)
            
            # print("director_team_id",director_team_id)
            # print("member_team_id",member_team_id)
            # print("is_head_sub_team",is_head_sub_team)
            # print("head_team_id",head_team_id)

            # member_set = set(member_team_id)
            # head_set = set(head_team_id)
            # not_in_head_team_id = list(member_set - head_set)
            # print("not_in_head_team_id",not_in_head_team_id)

            # print("team_id_filter",team_id)

            team_id = int(team_id)

            if team_id in director_team_id:
                # print("--director")

                sale_target_query = (
                    db.query(models.Sales_Targets)
                    .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                    .filter(
                        models.Sales_Targets.team_id == team_id,
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    ).all()
                ) 

                for target in sale_target_query:
                    total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))  

            elif team_id in member_team_id and team_id not in head_team_id:
                # print("--member")
                
                sale_target_query = (
                    db.query(models.Sales_Targets)
                    .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                    .filter(
                        models.Sales_Targets.team_id == team_id,
                        models.Sales_Targets.user_id == current_user,
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    )
                    .all()
                )
                    
                for target in sale_target_query:
                    total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))   
            
            elif team_id in head_team_id and is_head_sub_team == True:
                # print("--head")

                total_target_head = 0
                total_target_sub_member = 0

                user_members = user_team_child(current_user, team_id, db)

                sale_target_query = (
                    db.query(models.Sales_Targets)
                    .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                    .filter(
                        models.Sales_Targets.team_id == team_id,
                        models.Sales_Targets.user_id.in_(user_members),
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                )
                .all()
                )

                for target in sale_target_query:
                    total_target_sub_member += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
                    # print("total_target_sub_member", total_target_sub_member)

                head_target_query = (
                    db.query(models.Sales_Targets)
                    .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                    .filter(
                        models.Sales_Targets.team_id == team_id,
                        models.Sales_Targets.user_id == current_user,
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    )
                    .first()
                )

                if head_target_query is not None:
                    total_target_head = float(db.scalar(func.PGP_SYM_DECRYPT(head_target_query.value, key)))
                else:
                    # Handle the case when head_target_query is None
                    total_target_head = 0 

                # total_target_head = float(db.scalar(func.PGP_SYM_DECRYPT(head_target_query.value, key)))
                # print("total_target_head", total_target_head)

                # sum total target
                # print("**total_target_head", total_target_head)
                # print("**total_target_sub_member", total_target_sub_member)
                total_target = total_target_head + total_target_sub_member
                # print("Total Target:", total_target)

        elif "user2" in role_permissions:
            total_target = 0
    
    elif user_id:

        if "admin" in role_permissions:

            # print("-----filter user id admin")
            sale_target_query = (
                db.query(models.Sales_Targets)
                .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                .filter(
                    models.Sales_Targets.user_id == int(user_id),
                    models.Teams.board_id == board_id,
                    extract('year', models.Sales_Targets.year) == year
                )
                .all()
            )
            for target in sale_target_query:
                total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))
        
        elif "user1" in role_permissions:

            # print("-----filter user id user1")

            director_member = []
            sub_team_member = []
            
            all_team_id = role_permissions['user1']['team_id']
            director_team_id = role_permissions['user1']['director']
            member_team_id = role_permissions['user1']['member']

            is_head_sub_team, head_team_id = check_head_sub_team(current_user, member_team_id, board_id, db)
                            
            # print("director_team_id", director_team_id)
            # print("member_team_id", member_team_id)
            # print("is_head_sub_team", is_head_sub_team)
            # print("head_team_id", head_team_id)

            for team in head_team_id:
                user_members = user_team_child(current_user, team, db)
                sub_team_member.append({'head_team_id': team, 'sub_team_member': user_members})

            for team in director_team_id:
                user_members = user_team_child(current_user, team, db)
                director_member.append({'director_team_id': team, 'director_member': user_members})

            # print("director_member", director_member)
            # print("sub_team_member", sub_team_member)

            if int(user_id) == current_user:
                sale_target_query = (
                    db.query(models.Sales_Targets)
                    .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                    .filter(
                        models.Sales_Targets.team_id.in_(all_team_id),
                        models.Sales_Targets.user_id == int(user_id),
                        models.Teams.board_id == board_id,
                        extract('year', models.Sales_Targets.year) == year
                    )
                    .all()
                )

                for target in sale_target_query:
                    total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))

            elif any(int(user_id) in team_info['director_member'] for team_info in director_member):
                for team_info in director_member:
                    director_team_id = team_info['director_team_id']
                    director_members = team_info['director_member']

                    if int(user_id) in director_members:
                        sale_target_query = (
                            db.query(models.Sales_Targets)
                            .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                            .filter(
                            models.Sales_Targets.team_id == director_team_id,
                            models.Sales_Targets.user_id == int(user_id),
                            models.Teams.board_id == board_id,
                            extract('year', models.Sales_Targets.year) == year
                            )
                            .all()
                        )

                        for target in sale_target_query:
                            total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))

            elif any(int(user_id) in team_info['sub_team_member'] for team_info in sub_team_member):
                for team_info in sub_team_member:
                    head_team_id = team_info['head_team_id']
                    sub_team_members = team_info['sub_team_member']

                    if int(user_id) in sub_team_members:
                        sale_target_query = (
                            db.query(models.Sales_Targets)
                            .join(models.Teams, models.Teams.id == models.Sales_Targets.team_id)
                            .filter(
                            models.Sales_Targets.team_id == head_team_id,
                            models.Sales_Targets.user_id == int(user_id),
                            models.Teams.board_id == board_id,
                                extract('year', models.Sales_Targets.year) == year
                            )
                            .all()
                        )

                        for target in sale_target_query:
                            total_target += float(db.scalar(func.PGP_SYM_DECRYPT(target.value, key)))

        elif "user2" in role_permissions:
            total_target = 0

    # customer individual
    individual_customer_ids_query = db.query(models.Customers.id).filter(models.Customers.customer_type == "individual").all()
    individual_customer_ids = list(itertools.chain(*individual_customer_ids_query))

    # customer and contact latest log
    customer_latest_log = db.query(func.max(models.Customer_Logs.id)).filter(models.Users.company_id==company_id, models.Customers.user_id==models.Users.id,\
        models.Customer_Logs.customer_id==models.Customers.id).group_by(models.Customers.id).all()
    customer_latest_log = list(itertools.chain(*customer_latest_log))
    contact_latest_log = db.query(func.max(models.Contact_Logs.id)).filter(models.Users.company_id==company_id, models.Contacts.user_id==models.Users.id,\
        models.Contact_Logs.contact_id==models.Contacts.id).group_by(models.Contacts.id).all()
    contact_latest_log = list(itertools.chain(*contact_latest_log))

    pipeline_data = []
    remaining_pipeline = 0
    win_value = 0
    lose_value = 0
    all_value = 0

    if view_type == "all":  
        for pipeline in pipeline_query:

            print("pipeline name:",pipeline.name)

            deal_query = base_deal_query.filter(models.Deal_Logs.pipeline_id == pipeline.id)

            # sales_target_query = db.query(func.coalesce(func.sum(cast(func.PGP_SYM_DECRYPT(models.Sales_Targets.value, key), Float)), 0))\
            #     .filter(extract("year", models.Sales_Targets.year)==year, models.Sales_Targets.team_id==models.Teams.id, models.Teams.board_id==board_id)

            # role filter
            conditions = []
            # sale_target_conditions = []
            deal_owner_query = (models.Deal_Logs.user_id==models.Users.id)
            if role_name != 'admin' and role_name != 'superadmin':
                director_team_query = db.query(models.Teams.id).filter(models.Teams.id==models.Team_Members.team_id, models.Team_Members.user_id==user_query.id, models.Team_Members.position=='director').all()
                director_team_id = list(itertools.chain(*director_team_query))
                director_user_query = db.query(models.Users.id).filter(models.Users.id==models.Team_Members.user_id, models.Team_Members.team_id.in_(director_team_id), models.Users.id!=current_user).all()
                director_user_id = list(itertools.chain(*director_user_query))
                head_sub_team_query = db.query(models.Team_Members)\
                    .filter(models.Team_Members.parent_id==user_query.id, ~models.Team_Members.team_id.in_(director_team_id)).all()
                head_sub_team_id = [team.team_id for team in head_sub_team_query]
                if len(director_team_query) > 0:
                    for team_id in director_team_id:
                        child = user_team_child(user_query.id, team_id, db)
                        conditions.append(and_(models.Deal_Logs.user_id.in_(child), models.Deal_Logs.team_id==team_id))
                        # sale_target_conditions.append(and_(models.Sales_Targets.user_id.in_(child), models.Sales_Targets.team_id==team_id))
                if len(head_sub_team_query) > 0:
                    for team_id in head_sub_team_id:
                        child = user_team_child(user_query.id, team_id, db)
                        conditions.append(and_(models.Deal_Logs.user_id.in_(child), models.Deal_Logs.team_id==team_id))
                        # sale_target_conditions.append(and_(models.Sales_Targets.user_id.in_(child), models.Sales_Targets.team_id==team_id))
                deal_owner_query = or_(and_(models.Deal_Logs.user_id.in_(director_user_id), models.Deal_Logs.team_id.in_(director_team_id)),\
                    models.Deal_Logs.user_id==current_user, models.Deal_Members.user_id==current_user, models.Deal_Presales.user_id==current_user, *conditions)
                # sales_target_query = sales_target_query.filter(
                #     or_(models.Sales_Targets.user_id==current_user, *sale_target_conditions))
            deal_query = deal_query.filter(deal_owner_query)

            # filter focus, commit, owner, member
            if len(deal_filter_option) > 0:
                if 'focus' in deal_filter:
                    if team_id and user_id:
                        childs = []
                        sub_team = db.query(models.Team_Members.team_id).filter(models.Team_Members.parent_id==user_id).all()
                        sub_team_id = [team.team_id for team in sub_team]
                        for team_id in sub_team_id:
                            child = user_team_child(user_id, team_id, db)
                            childs = childs + child
                        childs.append(user_id)
                        deal_query = deal_query.filter(models.Deal_Focus.user_id==current_user)
                        deal_query = deal_query.filter(models.Deal_Logs.user_id.in_(childs))
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.user_id.in_(childs))
                    elif user_id:
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.user_id==user_id)
                        deal_query = deal_query.filter(models.Deal_Focus.user_id==current_user)
                        deal_query = deal_query.filter(models.Deal_Logs.user_id==user_id)
                    elif team_id:
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.team_id==team_id)
                        deal_query = deal_query.filter(models.Deal_Focus.user_id==current_user)
                        deal_query = deal_query.filter(models.Deal_Logs.team_id==team_id)
                    else:
                        deal_query = deal_query.filter(models.Deal_Focus.user_id==current_user)
                if 'commit' in deal_filter:
                    if team_id and user_id:
                        childs = []
                        sub_team = db.query(models.Team_Members.team_id).filter(models.Team_Members.parent_id==user_id).all()
                        sub_team_id = [team.team_id for team in sub_team]
                        for team_id in sub_team_id:
                            child = user_team_child(user_id, team_id, db)
                            childs = childs + child
                        childs.append(user_id)
                        deal_query = deal_query.filter(models.Deal_Logs.commit_status==True, models.Deal_Logs.user_id.in_(childs))
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.user_id.in_(childs))                    
                    elif user_id:
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.user_id==user_id)
                        deal_query = deal_query.filter(models.Deal_Logs.commit_status==True, models.Deal_Logs.user_id==user_id)
                    elif team_id:
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.team_id==team_id)
                        deal_query = deal_query.filter(models.Deal_Logs.commit_status==True, models.Deal_Logs.team_id==team_id)
                    else:
                        deal_query = deal_query.filter(models.Deal_Logs.commit_status==True)
                if 'owner' in deal_filter:
                    if team_id and user_id:
                        childs = []
                        sub_team = db.query(models.Team_Members.team_id).filter(models.Team_Members.parent_id==user_id).all()
                        sub_team_id = [team.team_id for team in sub_team]
                        for team_id in sub_team_id:
                            child = user_team_child(user_id, team_id, db)
                            childs = childs + child
                        childs.append(user_id)
                        deal_query = deal_query.filter(models.Deal_Logs.user_id.in_(childs))
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.user_id.in_(childs))                    
                    elif user_id:
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.user_id==user_id)
                        deal_query = deal_query.filter(models.Deal_Logs.user_id==user_id)
                    elif team_id:
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.team_id==team_id)
                        deal_query = deal_query.filter(models.Deal_Logs.team_id==team_id)
                if 'member' in deal_filter:
                    if team_id and user_id:
                        childs = []
                        sub_team = db.query(models.Team_Members.team_id).filter(models.Team_Members.parent_id==user_id).all()
                        sub_team_id = [team.team_id for team in sub_team]
                        for team_id in sub_team_id:
                            child = user_team_child(user_id, team_id, db)
                            childs = childs + child
                        childs.append(user_id)
                        deal_query = deal_query.filter(models.Deal_Members.user_id.in_(childs))
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.user_id.in_(childs))
                    elif user_id:
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.user_id==user_id)
                        deal_query = deal_query.filter(models.Deal_Members.user_id==user_id)
                    elif team_id:
                        sales_target_query = sales_target_query.filter(models.Sales_Targets.team_id==team_id)
                        user_id = db.query(models.Users).filter(models.Users.id==models.Team_Members.user_id, models.Team_Members.team_id==team_id).all()
                        deal_query = deal_query.filter(models.Deal_Members.user_id.in_([user.id for user in user_id]))
            elif len(deal_filter_option) == 0:
                if user_id and team_id:
                    childs = []
                    sub_team = db.query(models.Team_Members.team_id).filter(models.Team_Members.parent_id==user_id).all()
                    sub_team_id = [team.team_id for team in sub_team]
                    for team_id in sub_team_id:
                        child = user_team_child(user_id, team_id, db)
                        childs = childs + child
                    childs.append(user_id)
                    deal_query = deal_query.filter(models.Deal_Logs.user_id.in_(childs))
                    sales_target_query = sales_target_query.filter(models.Sales_Targets.user_id.in_(childs))
                elif team_id:
                    sales_target_query = sales_target_query.filter(models.Sales_Targets.team_id==team_id)
                    deal_query = deal_query.filter(models.Deal_Logs.team_id==team_id)
                elif user_id:
                    sales_target_query = sales_target_query.filter(models.Sales_Targets.user_id==user_id)
                    deal_query = deal_query.filter(models.Deal_Logs.user_id==user_id)

            # filter time
            if len(quarter_values) > 0:
                quarter_condition = []
                for q in quarter_values:
                    if int(q) == 1:
                        quarter_condition.append(
                            and_(func.date(models.Deal_Logs.close_datetime) >= str(year) + '-' + '1' + '-' + '1', func.date(models.Deal_Logs.close_datetime) < str(year) + '-' + '4' + '-' + '1')
                        )
                    elif int(q) == 2:
                        quarter_condition.append(
                            and_(func.date(models.Deal_Logs.close_datetime) >= str(year) + '-' + '4' + '-' + '1', func.date(models.Deal_Logs.close_datetime) < str(year) + '-' + '7' + '-' + '1')
                        )
                    elif int(q) == 3:
                        quarter_condition.append(
                            and_(func.date(models.Deal_Logs.close_datetime) >= str(year) + '-' + '7' + '-' + '1', func.date(models.Deal_Logs.close_datetime) < str(year) + '-' + '10' + '-' + '1')
                        )
                    elif int(q) == 4:
                        quarter_condition.append(
                            and_(func.date(models.Deal_Logs.close_datetime) >= str(year) + '-' + '10' + '-' + '1', func.date(models.Deal_Logs.close_datetime) < str(int(year)+1) + '-' + '1' + '-' + '1')
                        )
                deal_query = deal_query.filter(or_(*quarter_condition))
            if month:
                deal_query = deal_query.filter(extract("month", models.Deal_Logs.close_datetime) == month)
            if estCloseDateStart:
                deal_query = deal_query.filter(func.date(models.Deal_Logs.close_datetime) >= estCloseDateStart)
            if estCloseDateEnd:
                deal_query = deal_query.filter(func.date(models.Deal_Logs.close_datetime) <= estCloseDateEnd)
            if min_value:
                deal_query = deal_query.filter(cast(func.PGP_SYM_DECRYPT(models.Deal_Logs.value, key), Float) >= min_value)
            if max_value:
                deal_query = deal_query.filter(cast(func.PGP_SYM_DECRYPT(models.Deal_Logs.value, key), Float) <= max_value)
            if project:
                deal_query = deal_query.filter(func.PGP_SYM_DECRYPT(models.Deal_Logs.project, key).ilike(f"%{project}%"))
            if customer_type == 'individual':
                contact_query = db.query(func.PGP_SYM_DECRYPT(models.Contact_Logs.name, key), models.Contact_Logs.contact_id)\
                    .join(models.Contacts, models.Contacts.id==models.Contact_Logs.contact_id)\
                    .join(models.Customers, models.Customers.id==models.Contacts.customer_id)\
                    .filter(
                        models.Customers.customer_type == 'individual',
                        models.Customers.user.has(models.Users.company_id==company_id),
                        func.PGP_SYM_DECRYPT(models.Contact_Logs.name, key).ilike(f"%{customer_value}%"),
                        models.Contact_Logs.id.in_(contact_latest_log),
                    ).all() 
                contact_id_list = [contact[1] for contact in contact_query]
                deal_query = deal_query.filter(models.Deal_Logs.contact_id.in_(contact_id_list))
            elif customer_type == 'company':
                customer_query = db.query(func.PGP_SYM_DECRYPT(models.Customer_Logs.name, key), models.Customer_Logs.customer_id)\
                    .join(models.Customers, models.Customers.id==models.Customer_Logs.customer_id)\
                    .filter(
                        models.Customers.customer_type == 'company',
                        models.Customers.user.has(models.Users.company_id==company_id),
                        func.PGP_SYM_DECRYPT(models.Customer_Logs.name, key).ilike(f"%{customer_value}%"),
                        models.Customer_Logs.id.in_(customer_latest_log),
                    ).all()
                
                customer_id_list = [customer[1] for customer in customer_query]
                
                deal_query = deal_query.join(models.Contacts, models.Contacts.id == models.Deal_Logs.contact_id)\
                                    .join(models.Customers, models.Customers.id == models.Contacts.customer_id)\
                                    .filter(models.Customers.id.in_(customer_id_list))

            if len(product_values) > 0:
                deal_query = deal_query.outerjoin(models.Deal_Products, models.Deal_Products.deal_id == models.Deal_Logs.deal_id)\
                    .filter(models.Deal_Products.product_id.in_(product_values))

            pipeline_deals = deal_query.all()
            total_deal = len(pipeline_deals)
            total_value = 0
            un_select_pipeline = 0

            # remaining pipeline & remaining target
            for deal in pipeline_deals:
                total_value += float(db.scalar(func.PGP_SYM_DECRYPT(deal.value, key)))
                if pipeline.select_pipeline == False and pipeline.probability != 100 and pipeline.probability != 0:
                    un_select_pipeline = total_value
                if pipeline.probability == 100:
                    win_value = total_value
                if pipeline.probability == 0:
                    lose_value = total_value
            remaining_pipeline += total_value
            
            current_date = datetime.now().date()
            deal_data = []
            user_data_for_pipeline = []

            # deal in user pipeline
            user_data_query = db.query(
                models.Users.id.label("userId"),
                models.Users.name.label("name"),
                models.Users.photo.label("photo"),
                func.count(models.Deal_Logs.deal_id).label("totalDeal"),
            ).join(models.Deal_Logs, models.Deal_Logs.user_id == models.Users.id)\
            .filter(models.Deal_Logs.pipeline_id == pipeline.id)\
            .group_by(models.Users.id, models.Users.name)
            user_data_list = user_data_query.all()
        
            # deal data
            for deal in pipeline_deals:
                is_deal_in_focus = db.query(models.Deal_Focus).filter(models.Deal_Focus.deal_id == deal.deal_id, models.Deal_Focus.user_id == current_user).first() is not None 
                user = db.query(models.Users).filter(models.Users.id == deal.user_id).first()
                lose_type = db.query(models.Lose_Types).filter(models.Lose_Types.company_id == company_id,models.Lose_Types.id == deal.lose_type_id).first() 
                
                if lose_type is not None:
                    if lose_type.name is not None:
                        lose_type_name = lose_type.name
                    else:
                        lose_type_name = ''
                else:
                    lose_type_name = ''
                
                customer_type_check = (
                    db.query(models.Customers.customer_type)
                    .join(models.Contacts, models.Customers.id == models.Contacts.customer_id)
                    .filter(models.Contacts.id == deal.contact_id)
                    .scalar()
                )
                if customer_type_check == "company":
                    customer_query = (db.query(models.Customers, models.Customer_Logs)
                        .join(models.Contacts, models.Contacts.customer_id == models.Customers.id)
                        .join(models.Contact_Logs, models.Contact_Logs.contact_id == models.Contacts.id)
                        .join(models.Customer_Logs, models.Customer_Logs.customer_id == models.Customers.id)
                        .filter(models.Contact_Logs.contact_id == deal.contact_id, models.Customers.customer_type == "company")
                        .order_by(desc(models.Customer_Logs.id))
                        .first())                        
                elif customer_type_check == "individual":
                    customer_query = (db.query(models.Contacts, models.Contact_Logs)
                        .join(models.Customers, models.Customers.id == models.Contacts.customer_id)
                        .join(models.Customer_Logs, models.Customer_Logs.customer_id == models.Customers.id)
                        .join(models.Contact_Logs, models.Contact_Logs.contact_id == models.Contacts.id)
                        .filter(models.Contact_Logs.contact_id == deal.contact_id, models.Customers.customer_type == "individual")
                        .order_by(desc(models.Customer_Logs.id))
                        .first())

                customer, customer_logs = customer_query

                commit_datetime = deal.commit_datetime
                time_difference = commit_datetime.date() - current_date  
                days_difference = time_difference.days              

                if deal is not None and deal.create_datetime is not None:
                    create_date = deal.create_datetime.strftime("%d %b %Y")
                else:
                    create_date = None

                deal_age_day = (datetime.now() - deal.create_datetime).days
                if deal_age_day < 0:
                    deal_age_day = 0

                deal_age_query = db.query(models.Deal_Ages.name, models.Deal_Ages.id)\
                    .filter(and_(models.Deal_Ages.board_id == board_id, models.Deal_Ages.age <= deal_age_day))\
                    .order_by(desc(models.Deal_Ages.age)).first()

                deal_age_name, deal_age_id = deal_age_query

                deal_health = db.query(models.Deal_Healths).filter(models.Deal_Healths.deal_age_id == deal_age_id, models.Deal_Healths.deal_probability_id == deal.deal_probability_id).first()
                deal_health_id = deal_health.id if deal_health is not None else None
                deal_health_photo = deal_health.photo if deal_health is not None else None
                
                deal_probability__ = db.query(models.Deal_Probabilities).filter(models.Deal_Probabilities.id == deal.deal_probability_id).first()
                
                # pipeline pending
                pipeline_request_approval = db.query(
                    models.Request_Approvals.request_value
                ).outerjoin(
                    models.Deal_Logs, models.Deal_Logs.id == models.Request_Approvals.deal_log_id
                ).filter(
                    models.Deal_Logs.deal_id == deal.deal_id,
                    models.Request_Approvals.status == 'pending',
                    models.Request_Approvals.request_title == 'pipeline'
                ).order_by(desc(models.Request_Approvals.id)).first()

                # value pending
                value_request_approval = db.query(
                    models.Request_Approvals.request_value
                ).outerjoin(
                    models.Deal_Logs, models.Deal_Logs.id == models.Request_Approvals.deal_log_id
                ).filter(
                    models.Deal_Logs.deal_id == deal.deal_id,
                    models.Request_Approvals.status == 'pending',
                    models.Request_Approvals.request_title == 'value'
                ).order_by(desc(models.Request_Approvals.id)).first()

                # close date pending
                close_date_request_approval = db.query(
                    models.Request_Approvals.request_value
                ).outerjoin(
                    models.Deal_Logs, models.Deal_Logs.id == models.Request_Approvals.deal_log_id
                ).filter(
                    models.Deal_Logs.deal_id == deal.deal_id,
                    models.Request_Approvals.status == 'pending',
                    models.Request_Approvals.request_title == 'date'
                ).order_by(desc(models.Request_Approvals.id)).first()

                # uncommit pending
                commit_request_approval = db.query(
                    models.Request_Approvals.request_value
                ).outerjoin(
                    models.Deal_Logs, models.Deal_Logs.id == models.Request_Approvals.deal_log_id
                ).filter(
                    models.Deal_Logs.deal_id == deal.deal_id,
                    models.Request_Approvals.status == 'pending',
                    models.Request_Approvals.request_title == 'uncommit'
                ).order_by(desc(models.Request_Approvals.id)).first()

                deal_health_display = db.query(models.Deal_Healths_Display).filter(models.Deal_Healths_Display.user_id == current_user, models.Deal_Healths_Display.board_id == board_id).first()
                deal_health_display = True if deal_health_display is None else deal_health_display.display_deal_health

                deal_data.append({
                        "order": len(deal_data) + 1,
                        "deal_log_id": deal.id,
                        "dealId": deal.deal_id,
                        "team_id": deal.team_id,
                        "user_id": deal.user_id,
                        "photo": user.photo,
                        "pipeline_id": deal.pipeline_id,
                        "countDown": days_difference,
                        "project": db.scalar(func.PGP_SYM_DECRYPT(deal.project, key)),
                        "customer": db.scalar(func.PGP_SYM_DECRYPT(customer_logs.name, key)),
                        "customer_type": str(customer_type),
                        "value": f'{int(float(db.scalar(func.PGP_SYM_DECRYPT(deal.value, key)))):,}',
                        "estimate_closedate": deal.commit_datetime.strftime("%d %b %Y"),
                        "edit_datetime": str(deal.edit_datetime),
                        "closeDate": deal.close_datetime.strftime("%d %b %Y"),
                        "createDate" : create_date,
                        "probability": deal.probability_value if deal.probability_value is not None else '',
                        "ownerName": user.name,
                        "focus_deal": str(is_deal_in_focus),
                        "commit_status": str(deal.commit_status),
                        "ref_number": deal.so_number if deal.so_number is not None else '',
                        "lose_type": lose_type_name,
                        "deal_health_id": deal_health_id,
                        "deal_health_photo": deal_health_photo,
                        'deal_health_data': { 'probability': {'name': deal_probability__.name, 'value': deal_probability__.probability}, "age" : {'name': deal_age_name, 'value': deal_age_day}},
                        'is_moveable': str(False) if pipeline_request_approval is not None else str(True),
                        'is_value_approval': str(False) if value_request_approval is not None else str(True),
                        'waiting_value': f'{int(float(value_request_approval.request_value)):,}' if value_request_approval is not None else '',
                        'is_date_approval': str(False) if close_date_request_approval is not None else str(True),
                        'waiting_date': close_date_request_approval.request_value if close_date_request_approval is not None else '',
                        'is_commit_approval': str(False) if commit_request_approval is not None else str(True),
                        'is_health_display': str(deal_health_display)
                })

            # won, lose pipeline            
            if pipeline.probability == 100 or pipeline.probability == 0:
                for user_data_row in user_data_list:
                    user_deals_for_pipeline = [deal for deal in deal_data if deal["user_id"] == user_data_row.userId]
                    user_total_value = sum(int(deal["value"].replace(',', '')) for deal in user_deals_for_pipeline)

                    user_data_for_pipeline.append({
                        "userId": user_data_row.userId,
                        "photo": user_data_row.photo,
                        "name": user_data_row.name,
                        "totalDeal": len(user_deals_for_pipeline),
                        "totalValue": f'{user_total_value:,}',
                        "dealData": user_deals_for_pipeline
                    })

                pipeline_data.append({
                    "pipeline_id": pipeline.id,
                    "name": pipeline.name,
                    "probability":pipeline.probability,
                    "totalDeal": total_deal,
                    "totalValue": f'{int(total_value):,}',
                    "userData": user_data_for_pipeline
                })

            # other pipeline
            else:
                pipeline_data.append({
                    "pipeline_id": pipeline.id,
                    "name": pipeline.name,
                    "probability":pipeline.probability,
                    "select_pipeline": str(pipeline.select_pipeline),
                    "totalDeal": total_deal,
                    "totalValue": f'{int(total_value):,}',
                    "dealData": deal_data  
                })                   

            remaining_target = total_target - win_value
            if remaining_target < 0:
                remaining_target = 0

            win_lose = win_value + lose_value

            display_deal_health = db.query(models.Deal_Healths_Display).filter(models.Deal_Healths_Display.user_id == current_user, models.Deal_Healths_Display.board_id == board_id).first()
            all_value += un_select_pipeline
        
        print("return data")
        
        filter_deal = {
            "remainingPipeline": f'{int(remaining_pipeline-win_lose-all_value):,}',
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
        # print("1st create account")
        remaining_target = total_target - win_value
        win_lose = win_value + lose_value
        filter_deal = {
            "remainingPipeline": f'{int(remaining_pipeline-win_lose):,}',
            "remainingTarget": f'{int(remaining_target):,}',
            "totalTarget": f'{int(total_target):,}',
            "pipelineData": []
        }
        return filter_deal
