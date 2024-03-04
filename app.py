from flask import Flask, jsonify, request, abort
from flask_sqlalchemy import SQLAlchemy
from pymongo import MongoClient
from flask_redis import FlaskRedis
from bson import ObjectId
import json

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///site.db'
db = SQLAlchemy(app)

app.config['MONGO_URI'] = 'mongodb://localhost:27017/microservice_db'
mongo = MongoClient(app.config['MONGO_URI'])

app.config['Redis_URL'] = 'redis://localhost:6379/0'
redis = FlaskRedis(app)

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(20), unique=True, nullable=False)

@app.route('/api/sql_data', methods=['GET', 'POST'])
def sql_data():
    if request.method == 'GET':
        # Check if data is in Redis cache
        cached_data = redis.get('sql_data')
        if cached_data:
            user_list = jsonify(sql_data=json.loads(cached_data.decode('utf-8')))
        else:
            users = User.query.all()
            user_list = [{'id': user.id, 'username': user.username} for user in users]
            # Store data in Redis cache with a timeout of 60 seconds
            redis.setex('sql_data', 60, json.dumps(user_list))
        return user_list

    elif request.method == 'POST':
        data = request.get_json()
        new_user = User(username=data['username'])
        db.session.add(new_user)
        db.session.commit()
        # Clear the Redis cache when a new user is added
        redis.delete('sql_data')
        return jsonify(message='User added successfully')

@app.route('/api/sql_data/<int:user_id>', methods=['PUT'])
def update_sql_user(user_id):
    user = User.query.get_or_404(user_id)
    data = request.get_json()
    user.username = data['username']
    db.session.commit()
    # Clear the Redis cache when a user is updated
    redis.delete('sql_data')
    return jsonify(message=f'User {user_id} updated successfully')

@app.route('/api/nosql_data', methods=['GET', 'POST'])
def nosql_data():
    if request.method == 'GET':
        # Check if data is in Redis cache
        cached_data = redis.get('nosql_data')
        if cached_data:
            user_list = jsonify(nosql_data=json.loads(cached_data.decode('utf-8')))
        else:
            users = mongo.db.users.find()
            user_list = [{'id': str(user['_id']), 'username': user['username']} for user in users]
            # Store data in Redis cache with a timeout of 60 seconds
            redis.setex('nosql_data', 60, json.dumps(user_list))
        return user_list

    elif request.method == 'POST':
        data = request.get_json()
        new_user = {'username': data['username']}
        mongo.db.users.insert_one(new_user)
        # Clear the Redis cache when a new user is added
        redis.delete('nosql_data')
        return jsonify(message='User added successfully')

# @app.route('/api/nosql_data/<string:user_id>', methods=['PUT'])
# def update_nosql_user(user_id):
#     user = mongo.microservice_db.users.find_one({'_id': ObjectId(user_id)})

#     if user is None:
#         abort(404, description=f'User {user_id} not found')

#     data = request.get_json()
#     user['username'] = data['username']
#     mongo.microservice_db.users.update_one({'_id': ObjectId(user_id)}, {'$set': {'username': data['username']}})
#     updated_user = mongo.microservice_db.users.find_one({'_id': ObjectId(user_id)})

#     return jsonify(message=f'User {user_id} updated successfully', updated_user=updated_user)

# @app.route('/api/nosql_data/<string:user_id>', methods=['PUT'])
# def update_nosql_user(user_id):
#     user_id = user_id.strip("'")
#     print(f'recieved ObjectId: {user_id}')
#     try:
#         user_id_object = ObjectId(user_id)
#     except Exception as e:
#         abort(400, description=f'Invalid ObjectId: {e}')

#     user = mongo.microservice_db.users.find_one({'_id': user_id_object})

#     if user is None:
#         abort(404, description=f'User {user_id} not found')

#     data = request.get_json()
#     user['username'] = data['username']
    
#     # Update using the ObjectId
#     mongo.microservice_db.users.replace_one({'_id': user_id_object}, user)
    
#     updated_user = mongo.microservice_db.users.find_one({'_id': user_id_object})

#     return jsonify(message=f'User {user_id} updated successfully', updated_user=updated_user)

@app.route('/api/nosql_data/<string:user_id>', methods=['PUT'])
def update_nosql_user(user_id):
    # Remove single quotes from the received ObjectId
    user_id = user_id.strip("'")

    try:
        user_id_object = ObjectId(user_id)
    except Exception as e:
        abort(400, description=f'Invalid ObjectId: {e}')

    user = mongo.db.users.find_one({'_id': user_id_object})

    if user is None:
        abort(404, description=f'User {user_id} not found')

    data = request.get_json()
    user['username'] = data['username']

    # Update using the ObjectId
    mongo.db.users.replace_one({'_id': user_id_object}, user)

    updated_user = mongo.db.users.find_one({'_id': user_id_object})

    # Convert ObjectId to string for JSON serialization
    updated_user['_id'] = str(updated_user['_id'])

    # Clear the Redis cache when a user is updated
    redis.delete('nosql_data')

    return jsonify(message=f'User {user_id} updated successfully', updated_user=updated_user)

#2 Phase Commit
def prepare_sql_transaction(username):
    try:
        new_user = User(username=username)
        db.session.add(new_user)
        db.session.flush()  # Ensure that the user has an ID before committing
        return new_user.id
    except Exception as e:
        # Log the error or handle it appropriately
        print(f"Error preparing SQL transaction: {e}")
        return None

def commit_sql_transaction(user_id):
    try:
        db.session.commit()
        return True
    except Exception as e:
        # Log the error or handle it appropriately
        print(f"Error committing SQL transaction: {e}")
        return False

def rollback_sql_transaction(user_id):
    try:
        # Rollback any changes
        db.session.rollback()
        return True
    except Exception as e:
        # Log the error or handle it appropriately
        print(f"Error rolling back SQL transaction: {e}")
        return False

def prepare_nosql_transaction(username):
    try:
        new_user = {'username': username}
        mongo.db.users.insert_one(new_user)
        return str(new_user['_id'])
    except Exception as e:
        # Log the error or handle it appropriately
        print(f"Error preparing NoSQL transaction: {e}")
        return None

def commit_nosql_transaction(user_id):
    try:
        return True
    except Exception as e:
        # Log the error or handle it appropriately
        print(f"Error committing NoSQL transaction: {e}")
        return False

def rollback_nosql_transaction(user_id):
    try:
        # Remove the user added during prepare
        mongo.db.users.delete_one({'_id': ObjectId(user_id)})
        return True
    except Exception as e:
        # Log the error or handle it appropriately
        print(f"Error rolling back NoSQL transaction: {e}")
        return False

@app.route('/api/microservice_commit', methods=['POST'])
def microservice_commit():
    try:
        data = request.get_json()
        username = data.get('username')

        # Phase 1: Prepare
        sql_user_id = prepare_sql_transaction(username)
        nosql_user_id = prepare_nosql_transaction(username)

        if sql_user_id is not None and nosql_user_id is not None:
            # Both databases prepared successfully

            # Phase 2: Commit
            if commit_sql_transaction(sql_user_id) and commit_nosql_transaction(nosql_user_id):
                return jsonify(message='Transaction committed successfully'), 200

        # If any phase failed, rollback both databases
        rollback_sql_transaction(sql_user_id)
        rollback_nosql_transaction(nosql_user_id)

        return jsonify(message='Transaction failed. Rollback performed'), 500

    except Exception as e:
        # Log the error or handle it appropriately
        print(f"Error in microservice_commit: {e}")
        return jsonify(message='Internal server error'), 500

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=False, host='0.0.0.0', port=5000)
