from flask import Flask, render_template, request, redirect, url_for, flash, session, request, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room
from platformapp import app
from flask_hashing import Hashing
from datetime import datetime, timedelta
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename
import connect
import mysql.connector
import re
import stripe
import pusher
import matplotlib.pyplot as plt
import io
import base64
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
stripe.api_key = 'sk_test_51QYh6VP7Gxz09J3WGjvC5F5EfLPc7X8HMr9WDquS4J9sdqAlinbFDWAKgIN4ZnDjuJLfaHCaKCV2TrkfdRSjtOj800a9Ycj1F8'
socketio = SocketIO(app)
hashing = Hashing(app)  #create an instance of hashing
app.secret_key = 'Secret Key'
# Salt value used to hash passwords
PASSWORD_SALT = 'ABCD1234'
# Default role assigned to new users upon registration.
DEFAULT_USER_ROLE = 'user'
db_connection = None

@socketio.on('join')
def handle_join(data=None):
    if data is None:
        print("No data provided!")
        return
    room = data.get('room')  # Safely access 'room'
    if room:
        join_room(room)
        print(f"User joined room: {room}")
    else:
        print("Room not specified!")


@socketio.on('message_sent')
def handle_message_sent(data):
    receiver_room = f"user_{data['receiver_id']}"
    emit('new_message', {'sender_id': data['sender_id']}, room=receiver_room)

def getCursor():
    """Gets a new dictionary cursor for the database.
    
    If necessary, a new database connection be created here and used for all
    subsequent to getCursor()."""
    global db_connection

    if db_connection is None or not db_connection.is_connected():
        db_connection = mysql.connector.connect(user=connect.dbuser, \
            password=connect.dbpass, host=connect.dbhost, auth_plugin='mysql_native_password',\
            database=connect.dbname, autocommit=True)
    
    cursor = db_connection.cursor(dictionary=True)
    
    return cursor

@app.route('/')
def home():
    if 'profile_picture' not in session or not session['profile_picture']:
        session['profile_picture'] = 'profile_pictures/default.jpg'
    return render_template('home.html')

@app.route('/about')
def about():
    if 'profile_picture' not in session or not session['profile_picture']:
        session['profile_picture'] = 'profile_pictures/default.jpg'
    return render_template('about.html')

# Initialize Pusher client
pusher_client = pusher.Pusher(
    app_id='1895791',
    key='338f3c8bfd25fa7eeb44',
    secret='6072841f298408c7220a',
    cluster='ap1',
    ssl=True
)

@app.route('/support')
def support():
    if 'user_id' not in session or 'role' not in session:
        flash("Please log in to access the support page.", "warning")
        return redirect('/login')

    current_user_id = session['user_id']
    role = session['role']
    
    cursor = getCursor()
    try:
        if role == 'user':
            # Fetch all admins sorted by unread messages
            cursor.execute("""
                SELECT u.user_id, u.username AS name, 
                       COALESCE(SUM(CASE WHEN iml.read_status = FALSE THEN 1 ELSE 0 END), 0) AS unread_count
                FROM users u
                LEFT JOIN instant_message_logs iml ON u.user_id = iml.sender_id AND iml.receiver_id = %s
                WHERE u.role = 'admin'
                GROUP BY u.user_id, u.username
                ORDER BY unread_count DESC
            """, (current_user_id,))
        elif role == 'admin':
            # Fetch all users sorted by unread messages
            cursor.execute("""
                SELECT u.user_id, u.username AS name, 
                       COALESCE(SUM(CASE WHEN iml.read_status = FALSE THEN 1 ELSE 0 END), 0) AS unread_count
                FROM users u
                LEFT JOIN instant_message_logs iml ON u.user_id = iml.sender_id AND iml.receiver_id = %s
                WHERE u.role = 'user'
                GROUP BY u.user_id, u.username
                ORDER BY unread_count DESC
            """, (current_user_id,))
        else:
            flash("Invalid role.", "danger")
            return redirect('/login')

        users = cursor.fetchall()

        return render_template('support.html', users=users, role=role)
    except Exception as e:
        print(f"Error fetching users: {e}")
        flash("An error occurred while fetching support data.", "danger")
        return redirect('/')
    finally:
        db_connection.close()

@app.route('/get-sorted-users', methods=['GET'])
def get_sorted_users():
    if 'user_id' not in session or 'role' not in session:
        return jsonify({'success': False, 'message': 'Unauthorized access.'}), 401

    current_user_id = session['user_id']
    role = session['role']

    cursor = getCursor()
    try:
        if role == 'user':
            cursor.execute("""
                SELECT u.user_id, u.username AS name, 
                       COALESCE(SUM(CASE WHEN iml.read_status = FALSE THEN 1 ELSE 0 END), 0) AS unread_count
                FROM users u
                LEFT JOIN instant_message_logs iml ON u.user_id = iml.sender_id AND iml.receiver_id = %s
                WHERE u.role = 'admin'
                GROUP BY u.user_id, u.username
                ORDER BY unread_count DESC
            """, (current_user_id,))
        elif role == 'admin':
            cursor.execute("""
                SELECT u.user_id, u.username AS name, 
                       COALESCE(SUM(CASE WHEN iml.read_status = FALSE THEN 1 ELSE 0 END), 0) AS unread_count
                FROM users u
                LEFT JOIN instant_message_logs iml ON u.user_id = iml.sender_id AND iml.receiver_id = %s
                WHERE u.role = 'user'
                GROUP BY u.user_id, u.username
                ORDER BY unread_count DESC
            """, (current_user_id,))
        else:
            return jsonify({'success': False, 'message': 'Invalid role.'}), 400

        users = cursor.fetchall()
        return jsonify({'success': True, 'users': users})
    except Exception as e:
        print(f"Error fetching sorted users: {e}")
        return jsonify({'success': False, 'message': 'Failed to fetch data.'}), 500
    finally:
        db_connection.close()


@app.route('/message/<int:id>', methods=['GET'])
def message(id):
    if 'user_id' not in session or 'role' not in session:
        flash("Please log in to access messages.", "warning")
        return redirect('/login')

    current_user_id = session['user_id']
    role = session['role']

    cursor = getCursor()
    try:
        # Mark messages as read
        cursor.execute("""
            UPDATE instant_message_logs
            SET read_status = TRUE
            WHERE ((sender_id = %s AND receiver_id = %s) OR (sender_id = %s AND receiver_id = %s)) 
            AND read_status = FALSE
        """, (current_user_id, id, id, current_user_id))
        db_connection.commit()

        # Fetch the conversation
        cursor.execute("""
            SELECT iml.*, 
                   sender.username AS sender_name, 
                   receiver.username AS receiver_name
            FROM instant_message_logs iml
            JOIN users sender ON iml.sender_id = sender.user_id
            JOIN users receiver ON iml.receiver_id = receiver.user_id
            WHERE (iml.sender_id = %s AND iml.receiver_id = %s) OR (iml.sender_id = %s AND iml.receiver_id = %s)
            ORDER BY timestamp ASC
        """, (current_user_id, id, id, current_user_id))
        messages = cursor.fetchall()

        # Fetch the recipient's name
        cursor.execute("SELECT username FROM users WHERE user_id = %s", (id,))
        recipient = cursor.fetchone()

        return render_template(
            'message.html', 
            messages=messages, 
            recipient=recipient, 
            recipient_id=id, 
            current_user_id=current_user_id, 
            role=role
        )
    except Exception as e:
        print(f"Error fetching messages: {e}")
        return jsonify({'error': 'Failed to fetch messages'}), 500
    finally:
        db_connection.close()


@app.route('/get-unread-counts', methods=['GET'])
def get_unread_counts():
    if 'user_id' not in session:
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401

    user_id = session['user_id']
    role = session['role']
    try:
        cursor = getCursor()

        # Query unread counts for relevant users
        if role == 'user':
            cursor.execute("""
                SELECT u.user_id, 
                       (SELECT COUNT(*) FROM instant_message_logs 
                        WHERE sender_id = u.user_id AND receiver_id = %s AND read_status = FALSE) AS unread_count
                FROM users u WHERE role = 'admin'
            """, (user_id,))
        elif role == 'admin':
            cursor.execute("""
                SELECT u.user_id, 
                       (SELECT COUNT(*) FROM instant_message_logs 
                        WHERE sender_id = u.user_id AND receiver_id = %s AND read_status = FALSE) AS unread_count
                FROM users u WHERE role = 'user'
            """, (user_id,))
        else:
            return jsonify({'success': False, 'error': 'Invalid role'}), 400

        users = cursor.fetchall()
        return jsonify({'success': True, 'users': users})
    except Exception as e:
        print(f"Error fetching unread counts: {e}")
        return jsonify({'success': False, 'error': 'Failed to fetch data'}), 500
    
@app.route('/send_message', methods=['POST'])
def send_message():
    if 'user_id' not in session or 'role' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.json
    message_content = data.get('message_content')
    recipient_id = data.get('recipient_id')
    current_user_id = session['user_id']

    cursor = getCursor()
    try:
        # Fetch the sender's username
        cursor.execute("SELECT username FROM users WHERE user_id = %s", (current_user_id,))
        sender_data = cursor.fetchone()
        sender_username = sender_data['username'] if sender_data else 'Unknown User'

        # Insert message into the database using sender_id and receiver_id
        cursor.execute("""
            INSERT INTO instant_message_logs (sender_id, receiver_id, message_content)
            VALUES (%s, %s, %s)
        """, (current_user_id, recipient_id, message_content))
        db_connection.commit()

        # Trigger Pusher event for real-time updates, including sender's username
        pusher_client.trigger('my-channel', 'new-message', {
            'sender_id': current_user_id,
            'recipient_id': recipient_id,
            'message_content': message_content,
            'username': sender_username  
        })

        return jsonify({'success': True})
    except Exception as e:
        print(f"Error sending message: {e}")
        return jsonify({'error': 'Failed to send message'}), 500
    finally:
        cursor.close()  
        db_connection.close()  

@app.route('/shopping')
def shopping():
    # Check if the user is an admin
    if session.get('role') == 'admin':
        return redirect(url_for('edit_order'))  # Redirect to the Edit Order page for admin users
    
    # Fetch products from the database for non-admin users
    cursor = getCursor()
    cursor.execute("SELECT product_id, name, price, image FROM products")
    products = cursor.fetchall()  # Fetch all the products from the query
    
    return render_template('shopping.html', products=products)


@app.route('/cart_count', methods=['POST'])
def cart_count():
    if 'user_id' not in session:
        return jsonify({'cart_count': 0})
    
    cart = session.get('cart', {})
    cart_count = sum(cart.values())  
    user_id = session['user_id']
    cursor = getCursor()
    cursor.execute("""
        SELECT SUM(quantity) AS cart_count
        FROM cart
        WHERE user_id = %s AND status = 'pending'
    """, (user_id,))
    cart_count = cursor.fetchone()['cart_count'] or 0

    return jsonify({'cart_count': cart_count})



@app.route('/add_to_cart/<int:product_id>', methods=['POST'])
def add_to_cart(product_id):
    if 'user_id' not in session:
        print("User ID not found in session.")
        return jsonify({'error': 'User not logged in'}), 403
    else:
        user_id = session['user_id']
        print(f"User ID: {user_id}")
    
    cursor = getCursor()
    try:
        print(f"Fetching product with ID: {product_id}")
        cursor.execute("SELECT product_id, price FROM products WHERE product_id = %s", (product_id,))
        product = cursor.fetchone()
        if not product:
            print(f"Product ID {product_id} not found in the database")
            return jsonify({'error': 'Product not found'}), 404

        print(f"Checking cart for user ID {user_id} and product ID {product_id}")
        cursor.execute("""
        SELECT cart_id, quantity, status 
        FROM cart 
        WHERE user_id = %s AND product_id = %s
        ORDER BY cart_id DESC
        LIMIT 1""", (user_id, product_id))
        cart_item = cursor.fetchone()

        if cart_item:
            if cart_item['status'] == 'pending':
                # Check if quantity is already at the maximum limit
                if cart_item['quantity'] >= 10:
                    print(f"User has already added 10 items of product ID {product_id}")
                    return jsonify({'error': 'You can only order a maximum of 10 items for this product.'}), 400

                # Update quantity for pending cart item
                new_quantity = cart_item['quantity'] + 1
                print(f"Updating quantity to {new_quantity} for pending item")
                cursor.execute("""
                    UPDATE cart 
                    SET quantity = %s 
                    WHERE cart_id = %s
                """, (new_quantity, cart_item['cart_id']))
            else:
                # Create a new cart entry if the item was already paid
                print("Adding new product to cart with status 'pending'")
                cursor.execute("""
                    INSERT INTO cart (user_id, product_id, quantity, unit_price, status) 
                    VALUES (%s, %s, %s, %s, %s)
                """, (user_id, product_id, 1, product['price'], 'pending'))
        else:
            # Add new item to cart
            print("Adding new product to cart")
            cursor.execute("""
                INSERT INTO cart (user_id, product_id, quantity, unit_price, status) 
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, product_id, 1, product['price'], 'pending'))

        db_connection.commit()
        print("Cart updated successfully")

        cursor.execute("SELECT SUM(quantity) AS cart_count FROM cart WHERE user_id = %s AND status = 'pending'", (user_id,))
        cart_count = cursor.fetchone()['cart_count'] or 0

        return jsonify({'cart_count': cart_count})

    except Exception as e:
        print(f"Error adding product to cart: {e}")
        return jsonify({'error': 'Failed to add product'}), 500

@app.route('/cart', methods=['GET', 'POST', 'DELETE'])
def cart():
    cursor=getCursor()
    user_id = session.get('user_id')  
    if not user_id:
        return redirect('/login')  # Redirect to login if user is not logged in

    if request.method == 'POST':
        # Update cart quantities in the database
        cart_data = request.json
        try:
            for item in cart_data:
                product_id = item['product_id']
                quantity = item['quantity']
                if quantity > 10:
                    return jsonify({'error': f'Maximum quantity for product {product_id} is 10'}), 400
                cursor.execute("""
                UPDATE cart 
                SET quantity = %s 
                WHERE user_id = %s AND product_id = %s AND status = 'pending'""", (quantity, user_id, product_id))

            db_connection.commit()
            return jsonify(success=True)
        except Exception as e:
            print(f"Error updating cart: {e}")
            return jsonify({'error': 'Failed to update cart'}), 500

    elif request.method == 'DELETE':
        # Remove an item from the cart
        data = request.json
        product_id = data.get('product_id')
        try:
            cursor.execute("""
            DELETE FROM cart 
            WHERE user_id = %s AND product_id = %s AND status = 'pending'""", (user_id, product_id))

            db_connection.commit()
            return jsonify(success=True)
        except Exception as e:
            print(f"Error removing item from cart: {e}")
            return jsonify({'error': 'Failed to remove item'}), 500

    try:
        # Fetch cart items for the current user
        cursor.execute("""
            SELECT 
                c.product_id, 
                p.name, 
                c.quantity, 
                c.unit_price 
            FROM cart c
            JOIN products p ON c.product_id = p.product_id
            WHERE c.user_id = %s AND c.status = 'pending'
        """, (user_id,))
        cart_items = cursor.fetchall()

        total_amount = sum(float(item['unit_price']) * int(item['quantity']) for item in cart_items)
        return render_template('cart.html', cart=cart_items, total_amount=total_amount)
    except Exception as e:
        print(f"Error fetching cart: {e}")
        return jsonify({'error': 'Failed to fetch cart'}), 500

@app.route('/save-address', methods=['GET'])
def save_address():
    cursor = getCursor()
    user_id = session.get('user_id')
    if not user_id:
        return redirect('/login')  # Redirect to login if user is not logged in

    # Capture the secondary address and amount from the URL parameters
    secondary_address = request.args.get('secondary_address', None)
    amount = request.args.get('amount', None)
    
    if secondary_address and amount:
        try:
            # Update the user's cart with the captured secondary_address in MySQL
            cursor.execute("""
                UPDATE cart 
                SET secondary_address = %s
                WHERE user_id = %s AND status = 'pending'
            """, (secondary_address, user_id))  # Correct parameter order
            db_connection.commit()
            
            print("Secondary address captured and saved.")

            # Pass the amount to the payment page
            return redirect(f'/payment?amount={amount}')  # Redirect to payment page with amount
        except Exception as e:
            print(f"Error saving address: {e}")
            return jsonify({"success": False, "error": str(e)}), 400
    else:
        return jsonify({"success": False, "error": "Missing address or amount."}), 400

@app.route('/payment', methods=['GET', 'POST']) 
def payment():
    cursor = getCursor()
    user_id = session.get('user_id')  
    if not user_id:
        return redirect('/login')  # Redirect to login if user is not logged in

    if request.method == 'POST':
        try:
            # Parse JSON data from the request
            data = request.get_json()
            total_amount = data.get('amount', 0)
            name = data.get('name', 'Anonymous')

            # Get differentAddress from the request data
            secondary_address = data.get('differentAddress', None)  # None if not provided

            print(f"Processing payment for {name}, amount: {total_amount}, address: {secondary_address}")

            # Update the status of all pending cart items to 'paid' for this user
            cursor.execute("""
                UPDATE cart 
                SET status = 'paid'
                WHERE user_id = %s AND status = 'pending'
            """, (user_id,))  # Correct parameter order
            db_connection.commit()
            
            session.pop('cart', None) 
            session['cart'] = []
            
            print("Cart items marked as paid.")

            # Clear the cart session if needed
            if 'cart' in session:
                print(f"Cart before clearing: {session.get('cart')}")
                session.pop('cart', None)
                print(f"Cart after clearing: {session.get('cart')}")
                print("Cart cleared after successful payment.")
            else:
                print("Cart was already empty.")

            return jsonify({"success": True}), 200

        except Exception as e:
            print(f"Error processing payment: {e}")
            return jsonify({"success": False, "error": str(e)}), 400

    # For GET request, display the payment page
    total_amount = request.args.get('amount', default=0, type=float)
    stripe_public_key = 'pk_test_51QYh6VP7Gxz09J3WjmXvxVQhI9dFleUahvOZ4AU0XWIpAEXdD66DaGVy9Y7bgPH7VPQuS0YNoOBB4i1kn5UgjpQn00c52DqTmr'
    return render_template('payment.html', stripe_public_key=stripe_public_key, total_amount=total_amount)


#4242 4242 4242 4242

@app.route('/create-payment-intent', methods=['POST'])
def create_payment_intent():
    try:
        data = request.json
        amount = int(data.get('amount', 0))  # Amount in cents
        currency = data.get('currency', 'usd')

        payment_intent = stripe.PaymentIntent.create(
            amount=amount,
            currency=currency,
            payment_method_types=['card']
        )

        print(f"Client Secret: {payment_intent['client_secret']}")  # Debug log

        return jsonify({
            'clientSecret': payment_intent['client_secret']
        })
    except Exception as e:
        print(f"Error creating PaymentIntent: {e}")
        return jsonify(error=str(e)), 403

@app.route('/register', methods=['POST'])
def register():
    username = request.form.get('register_user_id')
    email = request.form.get('register_email')
    password = request.form.get('register_password')
    confirm_password = request.form.get('register_confirm_password')

    # Input validation
    if not username or not email or not password or not confirm_password:
        flash('All fields are required.', 'danger')
        return render_template('home.html', error=True, register_error=True)

    if password != confirm_password:
        flash('Passwords do not match.', 'danger')
        return render_template('home.html', error=True, register_error=True)

    if not re.match(r'[^@]+@[^@]+\.[^@]+', email):
        flash('Invalid email address.', 'danger')
        return render_template('home.html', error=True, register_error=True)

    if not re.match(r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d).{8,}$', password):
        flash('Password must be at least 8 characters long, and include at least one uppercase letter, one lowercase letter, and one number.', 'danger')
        return render_template('home.html', error=True, register_error=True)

    # Database operations
    cursor = getCursor()
    try:
        cursor.execute('SELECT * FROM users WHERE username = %s OR email = %s', (username, email))
        account = cursor.fetchone()

        if account:
            flash('Username or email already exists.', 'danger')
            return render_template('home.html', error=True, register_error=True)

        hashed_password = hashing.hash_value(password, salt=PASSWORD_SALT)
        cursor.execute(
            'INSERT INTO users (username, email, password, role) VALUES (%s, %s, %s, %s)',
            (username, email, hashed_password, 'user')
        )
        db_connection.commit()
        flash('Registration successful! Please log in.', 'success')
        return render_template('home.html', error=False, register_error=False)

    except Exception as e:
        flash('An error occurred during registration. Please try again later.', 'danger')
        return render_template('home.html', error=True, register_error=True)

    finally:
        cursor.close()


@app.route('/login', methods=['GET', 'POST'])
def login():
    session.pop('login_error', None)  # Clear any previous error
    if request.method == 'POST':
        username = request.form['username']
        user_password = request.form['password']

        cursor = getCursor()
        cursor.execute('SELECT user_id, username, password, role, profile_picture FROM users WHERE username = %s', (username,))
        account = cursor.fetchone()
        cursor.close()

        if account:
            if hashing.check_value(account['password'], user_password, PASSWORD_SALT):
                session['loggedin'] = True
                session['user_id'] = account['user_id']
                session['username'] = account['username']
                session['role'] = account['role']
                session['profile_picture'] = account['profile_picture']  # Assuming 'user' is an instance of your User model


              
                return redirect(url_for('about'))
            else:
                flash('Incorrect password!', 'danger')
                return render_template('home.html',error=True)
        else:
            flash('Incorrect username!', 'danger')
            return render_template('home.html',error=True)

    return render_template('home.html')

@app.route('/logout')
def logout():
    session.pop('username', None)
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('home'))

# Define the upload folder path relative to the platformapp directory
UPLOAD_FOLDER = os.path.join('platformapp', 'static', 'profile_pictures')

# Ensure the folder exists at runtime
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@app.route('/profile', methods=['GET', 'POST'])
def profile():
    if 'username' not in session:
        flash('Please log in first.', 'warning')
        return redirect(url_for('home'))

    cursor = getCursor()
    cursor.execute("SELECT * FROM users WHERE username = %s", (session['username'],))
    user = cursor.fetchone()
    
    if not user:
        flash('User not found.', 'danger')
        return redirect(url_for('home'))

    if request.method == 'POST':
        first_name = request.form['first_name']
        last_name = request.form['last_name']
        email = request.form['email']
        password = request.form['password']
        confirm_password = request.form['confirm_password']
        profile_picture = request.files.get('profile_picture')
        country = request.form['country']
        city = request.form['city']
        address = request.form['address']

        if password:
            if not re.search(r'[A-Z]', password):
                flash('Password must contain at least one uppercase letter.', 'danger')
                return redirect(url_for('profile'))
            if not re.search(r'[a-z]', password):
                flash('Password must contain at least one lowercase letter.', 'danger')
                return redirect(url_for('profile'))
            if len(password) < 8:
                flash('Password must be at least 8 characters long.', 'danger')
                return redirect(url_for('profile'))
            if password != confirm_password:
                flash('The passwords do not match. Please try again.', 'danger')
                return redirect(url_for('profile'))

        update_fields = {
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'country': country,
            'city': city,
            'address': address,
        }

        if password:
            update_fields['password'] = hashing.hash_value(password, PASSWORD_SALT)

        if profile_picture and profile_picture.filename:  # Ensure a file is selected
            filename = secure_filename(profile_picture.filename)
            profile_path = os.path.join(UPLOAD_FOLDER, filename)
            profile_picture.save(profile_path)
            profile_picture_url = f'profile_pictures/{filename}'
            update_fields['profile_picture'] = profile_picture_url
            session['profile_picture'] = profile_picture_url  # Update session with new profile picture

        set_clause = ", ".join([f"{key} = %s" for key in update_fields.keys()])
        values = tuple(update_fields.values())

        cursor.execute(f"""
            UPDATE users
            SET {set_clause}
            WHERE username = %s
        """, (*values, session['username']))
        db_connection.commit()

        flash('Profile updated successfully!', 'success')
        return redirect(url_for('profile'))

    return render_template('profile.html', user=user)


@app.route('/zengping')
def zengping_details():
    skills = [
        "Experienced in common security monitoring systems (e.g., Zabbix, bastion hosts) and IPSec VPN configuration.",
        "Proficient in using security tools like Kali Linux, Nmap, MSF, and Hydra; familiar with web vulnerability tools such as Burp Suite and Sqlmap.",
        "Knowledgeable in web attack methods, including SQL injection, XSS, CSRF, and sensitive information leaks.",
        "Familiar with China's cybersecurity laws and regulations, as well as Tiered Protection 2.0 standards.",
        "Skilled in configuring firewalls, intrusion detection, and vulnerability scanning.",
        "Capable of Python scripting (requests, pandas) for penetration testing and basic automation tools; understands web front-end and back-end interaction.",
        "Familiar with OSI Layer 2/3 protocols such as OSPF, VLAN, TCP, and NAT."
    ]

    experiences = [
        {
            "role": "Cybersecurity Consultant",
            "company": "Beijing Moyun Technology Co., Ltd.",
            "year": "2024",
            "description": "Conducted penetration testing for a client’s authorized website, identified and reported two critical risks, and provided actionable insights to improve security."
        },
        {
            "role": "Security Analyst",
            "company": "Jiuku Music Company",
            "year": "2023",
            "description": "Performed vulnerability scanning, traffic monitoring, and real-time alerting using tools like Nmap and MSF. Collaborated with system owners to mitigate identified vulnerabilities."
        },
        {
            "role": "Systems Monitoring Specialist",
            "company": "Zabbix Monitoring Project",
            "year": "2023",
            "description": "Implemented and managed Zabbix-based website monitoring systems to ensure continuous availability and reliability."
        }
    ]

    return render_template('zengping_details.html', skills=skills, experiences=experiences)


@app.route('/yuan')
def yuan_details():
    technical_skills = [
        "Advanced Power BI, proficient with DAX, M code, and power query",
        "Proficient with Oracle SQL developer and MySQL",
        "Experienced with Python (Flask and pandas), HTML, JavaScript, CSS, Bootstrap, Git, PythonAnywhere",
        "Experienced with Snowflake",
        "Skilled at Power Apps and Power Automate",
        "Proficient with Microsoft Excel (advanced VBA/macros) and Microsoft Project",
        "Experienced with LEAN management and Agile",
        "Proficient with Photoshop and Camtasia"
    ]

    experiences = [
        {
            "role": "System Creator",
            "company": "Kāinga Ora",
            "year": "2024-Present",
            "description": "Engaged in digital transformation projects, managed the Delivery Optimization Program, and automated data collection using Python pandas."
        },
        {
            "role": "Business Analyst",
            "company": "Kāinga Ora",
            "year": "2023-2024",
            "description": "Led construction analysis, developed Power BI dashboards, and designed tools to streamline business processes."
        },
        {
            "role": "Asset Information Analyst",
            "company": "Kāinga Ora",
            "year": "2022-2023",
            "description": "Performed data research, created reports, and built data models for forecasting appliances demands."
        },
        {
            "role": "Business Development Specialist",
            "company": "Ford China",
            "year": "2014-2018",
            "description": "Tripled SSP sales volume, launched a new product, and managed promotional campaigns with a budget of 8 million RMB."
        }
    ]
    awards = [
        "Lean Six Sigma Yellow Belt - Kāinga Ora (2022)",
        "Director Award - Ford (2017)"
    ]
    return render_template('yuan_details.html', 
                           technical_skills=technical_skills,
                           experiences=experiences,
                           awards=awards)



@app.route('/insights')
def insights():
    try:
        
        db_settings = {
            "host": "localhost",
            "user": "root",
            "password": "Zm1990112",
            "database": "platform",
        }

        # Initialize SparkSession with the necessary MySQL driver
        spark = SparkSession.builder \
            .appName("DataAnalysis") \
            .config("spark.jars", "C:\\Users\\ly89757zm\\Documents\\YL\\13_python\\05_mysql connector_J\\mysql-connector-j-9.2.0\\mysql-connector-j-9.2.0.jar") \
            .getOrCreate()

        # JDBC URL and connection properties
        jdbc_url = f"jdbc:mysql://{db_settings['host']}/{db_settings['database']}?serverTimezone=UTC"
        connection_properties = {
            "user": db_settings["user"],
            "password": db_settings["password"],
            "driver": "com.mysql.cj.jdbc.Driver",
        }

        # Load MySQL tables into PySpark DataFrames
        users_df = spark.read.jdbc(url=jdbc_url, table="users", properties=connection_properties)
        cart_df = spark.read.jdbc(url=jdbc_url, table="cart", properties=connection_properties)

        # Users insights
        users_by_country = (
            users_df.groupBy("country")
            .count()
            .orderBy("count", ascending=False)
            .collect()
        )

        # Cart insights
        cart_df = cart_df.withColumn("quantity", col("quantity").cast("int"))
        cart_df = cart_df.withColumn("total_price", col("unit_price") * col("quantity"))
        
        cart_items_by_product = (
            cart_df.groupBy("product_id")
            .sum("quantity")
            .orderBy("sum(quantity)", ascending=False)
            .collect()
        )

        cart_total_price = cart_df.agg({"total_price": "sum"}).collect()[0][0]
        cart_total_quantity = cart_df.agg({"quantity": "sum"}).collect()[0][0]
        cart_avg_price = (
            cart_total_price / cart_total_quantity if cart_total_quantity and cart_total_quantity > 0 else 0
        )


        # Filter cart items with 'paid' status
        paid_cart_df = cart_df.filter(col("status") == "paid")
        
        # Extract year and month from order date
        paid_cart_df = paid_cart_df.withColumn("year", year("updated_at"))
        paid_cart_df = paid_cart_df.withColumn("month", month("updated_at"))
        
        # Calculate total sales by month
        sales_by_month = (
            paid_cart_df.groupBy("year", "month")
            .sum("total_price")
            .orderBy("year", "month")
            .collect()
        )
        # ---- Matplotlib Visuals ----
        
        # 1. Users by Country Bar Chart
        countries = [row['country'] for row in users_by_country]
        user_counts = [row['count'] for row in users_by_country]
        
        fig1, ax1 = plt.subplots(figsize=(8, 8))  # Adjust the figure size
        ax1.bar(countries, user_counts, color='skyblue')
        ax1.set_title("Users by Country")
        ax1.set_xlabel('')
        ax1.set_ylabel('')
        plt.xticks(rotation=45, ha="center", fontsize=8)
        ax1.yaxis.get_major_locator().set_params(integer=True)
        
        # Convert plot to PNG and encode it in base64
        img1 = io.BytesIO()
        FigureCanvas(fig1).print_png(img1)
        img1.seek(0)
        img1_base64 = base64.b64encode(img1.getvalue()).decode('utf-8')

        # 2. Cart Items by Product Bar Chart
        product_ids = [row['product_id'] for row in cart_items_by_product]
        quantities = [row['sum(quantity)'] for row in cart_items_by_product]
        
        fig2, ax2 = plt.subplots()
        ax2.bar(product_ids, quantities, color='lightgreen')
        ax2.set_title("Items by Product ID")
        ax2.set_xlabel("Product ID")
        ax2.set_ylabel("Quantity")
        plt.xticks(ha="center", fontsize=8)

        ax2.xaxis.get_major_locator().set_params(integer=True)
        
        # Convert plot to PNG and encode it in base64
        img2 = io.BytesIO()
        FigureCanvas(fig2).print_png(img2)
        img2.seek(0)
        img2_base64 = base64.b64encode(img2.getvalue()).decode('utf-8')

        # 3. Sales by Month Line Chart
        months = [f"{row['month']}-{row['year']}" for row in sales_by_month]
        total_sales = [row['sum(total_price)'] for row in sales_by_month]
        
        fig3, ax3 = plt.subplots(figsize=(10, 6))  # Adjust the figure size for better readability
        ax3.plot(months, total_sales, marker='o', color='purple', linestyle='-', linewidth=2, markersize=6)
        ax3.set_title("Sales by Month")
        ax3.set_xlabel('')
        ax3.set_ylabel("Sales ($)")
        ax3.tick_params(axis='x', rotation=45)
        
        # Convert plot to PNG and encode it in base64
        img3 = io.BytesIO()
        FigureCanvas(fig3).print_png(img3)
        img3.seek(0)
        img3_base64 = base64.b64encode(img3.getvalue()).decode('utf-8')


        
        return render_template(
            'insights.html',
            users_by_country=users_by_country,
            cart_items_by_product=cart_items_by_product,
            cart_avg_price=cart_avg_price,
            img1_base64=img1_base64,
            img2_base64=img2_base64,
            img3_base64=img3_base64,
        )

    except Exception as e:
        print(f"An error occurred: {e}")
        return render_template('insights.html', message="An error occurred while fetching data.")



@app.route('/edit-order', methods=['GET', 'POST'])
def edit_order():
    cursor = getCursor()
    
    if request.method == 'POST':
        cart_id = request.form.get('cart_id')
        if 'quantity' in request.form:
            quantity = int(request.form.get('quantity', 0))
            if quantity > 0:
                # Update the quantity in the cart
                cursor.execute("UPDATE cart SET quantity = %s WHERE cart_id = %s", (quantity, cart_id))
                flash("Quantity updated successfully!", "success")
            else:
                flash("Invalid quantity value.", "danger")
        elif 'remove' in request.form:
            # Remove the item from the cart
            cursor.execute("DELETE FROM cart WHERE cart_id = %s", (cart_id,))
            flash("Item removed successfully!", "success")
        
        # Redirect to avoid form resubmission
        return redirect(url_for('edit_order'))

    # Fetch unpaid items to display on the page
    cursor.execute("""
        SELECT c.cart_id, u.username, p.name AS product_name, c.quantity, c.unit_price, 
               (c.quantity * c.unit_price) AS total_price
        FROM cart c
        JOIN users u ON c.user_id = u.user_id
        JOIN products p ON c.product_id = p.product_id
        WHERE c.status = 'pending'
    """)
    unpaid_items = cursor.fetchall()
    
    return render_template('edit_order.html', unpaid_items=unpaid_items)


@app.route('/contact', methods=['GET', 'POST'])
def contact():
    if 'user_id' not in session:
        flash('Please log in first to access this page.', 'warning')
        return redirect(url_for('login'))  # Redirect to login page

    cursor = getCursor()
    
    # Check if the logged-in user is an admin
    cursor.execute("SELECT role FROM users WHERE user_id = %s", (session['user_id'],))
    user_role = cursor.fetchone()['role']

    if user_role == 'admin':
        # If the user is an admin, fetch all messages with user details
        cursor.execute("""
            SELECT users.user_id, users.first_name, users.last_name, users.email, contact_messages.message, contact_messages.submitted_at 
            FROM contact_messages
            JOIN users ON contact_messages.user_id = users.user_id
            ORDER BY contact_messages.submitted_at DESC
        """)
        messages = cursor.fetchall()
        return render_template('contact_admin.html', messages=messages)  # Admin view

    # For normal users, handle message submission
    if request.method == 'POST':
        message = request.form.get('message')
        user_id = session['user_id']

        if message:
            cursor.execute("INSERT INTO contact_messages (user_id, message) VALUES (%s, %s)", (user_id, message))
            db_connection.commit()
            flash('Your message has been sent successfully!', 'success')
            return redirect(url_for('contact'))  # Refresh to prevent duplicate submissions

    return render_template('contact.html')  # User view

@app.route('/service', methods=['GET', 'POST'])
def service():
    if 'user_id' not in session:
        flash("You need to log in to access services.", "danger")
        return redirect(url_for('login'))

    user_id = session['user_id']
    user_role = session.get('role', 'user')

    cursor = getCursor()

    if request.method == 'POST':
        service_id = request.form['service_id']
        details = request.form['details']
        quote_amount = request.form['quote_amount']

        query = """
        INSERT INTO user_service_requests (user_id, service_id, details, quote_amount, status)
        VALUES (%s, %s, %s, %s, 'Pending')
        """
        cursor.execute(query, (user_id, service_id, details, quote_amount))
        db_connection.commit()
        flash("Your quote request has been submitted!", "success")
        return redirect(url_for('service'))

    # Fetch available services
    cursor.execute("SELECT * FROM digital_services")
    services = cursor.fetchall()

    if user_role == 'user':
        cursor.execute("""
            SELECT r.request_id, s.name, r.details, r.quote_amount, r.status
            FROM user_service_requests r
            JOIN digital_services s ON r.service_id = s.service_id
            WHERE r.user_id = %s
        """, (user_id,))
        user_quotes = cursor.fetchall()
    else:
        cursor.execute("""
            SELECT r.request_id, u.username, u.email, s.name, r.details, r.quote_amount, r.status
            FROM user_service_requests r
            JOIN digital_services s ON r.service_id = s.service_id
            JOIN users u ON r.user_id = u.user_id
        """)
        admin_quotes = cursor.fetchall()

    cursor.close()

    return render_template(
        'service.html',
        services=services,
        user_quotes=user_quotes if user_role == 'user' else None,
        admin_quotes=admin_quotes if user_role == 'admin' else None,
        user_role=user_role
    )

@app.route('/update_quote/<int:request_id>', methods=['POST'])
def update_quote(request_id):
    if 'user_id' not in session:
        flash("You need to log in.", "danger")
        return redirect(url_for('login'))

    new_quote_amount = request.form['new_quote_amount']
    cursor = getCursor()
    
    # Ensure quote is still pending before updating
    cursor.execute("SELECT status FROM user_service_requests WHERE request_id = %s", (request_id,))
    quote = cursor.fetchone()
    
    if not quote or quote['status'] == 'Approved':
        flash("Quote cannot be updated after approval.", "danger")
        return redirect(url_for('service'))
    
    cursor.execute("UPDATE user_service_requests SET quote_amount = %s WHERE request_id = %s", 
                   (new_quote_amount, request_id))
    db_connection.commit()
    cursor.close()

    flash("Quote updated successfully.", "success")
    return redirect(url_for('service'))

@app.route('/approve_quote/<int:request_id>')
def approve_quote(request_id):
    if 'user_id' not in session or session.get('role') != 'admin':
        flash("Unauthorized access.", "danger")
        return redirect(url_for('service'))

    cursor = getCursor()
    cursor.execute("UPDATE user_service_requests SET status = 'Approved' WHERE request_id = %s", (request_id,))
    db_connection.commit()
    cursor.close()

    flash("Quote has been approved.", "success")
    return redirect(url_for('service'))

def get_quote_by_request_id(request_id):
    cursor = getCursor()  # Ensure you have a valid cursor from the database
    cursor.execute("""
        SELECT quote_amount FROM user_service_requests
        WHERE request_id = %s
    """, (request_id,))
    result = cursor.fetchone()
    if result:
        return result  # This should return a dictionary or tuple with the 'quote_amount'
    return None  # Return None if no result is found


@app.route('/process_service_payment/<int:request_id>', methods=['POST'])
def process_service_payment(request_id):
    # Get the quote details based on request_id
    quote = get_quote_by_request_id(request_id)
    
    if quote is None:
        # Handle case if no quote is found
        return "Quote not found", 404

    user_id = session.get('user_id')
    cursor = getCursor()

    # Update the status to 'Paid' in the database
    cursor.execute("""
        UPDATE user_service_requests
        SET status = 'Paid'
        WHERE request_id = %s AND user_id = %s
    """, (request_id, user_id))
    db_connection.commit()
    
    # Redirect to payment page with the quote amount
    return redirect(url_for('service_payment', quote_amount=quote['quote_amount']))

stripe_public_key = 'pk_test_51QYh6VP7Gxz09J3WjmXvxVQhI9dFleUahvOZ4AU0XWIpAEXdD66DaGVy9Y7bgPH7VPQuS0YNoOBB4i1kn5UgjpQn00c52DqTmr'

@app.route('/service_payment')
def service_payment():
    quote_amount = request.args.get('quote_amount')  # Get quote_amount from the query parameter
    return render_template('service_payment.html', 
                           stripe_public_key=stripe_public_key, 
                           quote_amount=quote_amount)



if __name__ == '__main__':
    app.run(debug=True)