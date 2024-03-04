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


if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)
