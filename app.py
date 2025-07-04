import os
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, MetaData, Table, select, func, text
from flask_cors import CORS

# Configuración de la app
app = Flask(__name__)
CORS(app)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("Define la variable de entorno DATABASE_URL con tu conexión a Postgres")
engine = create_engine(DATABASE_URL, echo=True)

# Reflexión de esquema existente
meta = MetaData()
meta.reflect(bind=engine)
# Tablas
Usuario      = meta.tables['Usuario']
Cliente      = meta.tables['Cliente']
Trabajador   = meta.tables['Trabajador']
Repartidor   = meta.tables['Repartidor']
Administrador= meta.tables['Administrador']
Menu         = meta.tables['Menu']
Plato        = meta.tables['Plato']
Pertenece    = meta.tables['Pertenece']
ZonaEntrega  = meta.tables['ZonaEntrega']
Pedido       = meta.tables['Pedido']
Tiene        = meta.tables['Tiene']
Hace         = meta.tables['Hace']
Vive         = meta.tables['Vive']
Cubre        = meta.tables['Cubre']

# Helper de paginación
def paginate(table):
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 20))
    offset = (page - 1) * limit
    with engine.connect() as conn:
        total = conn.execute(select(func.count()).select_from(table)).scalar()
        rows = conn.execute(select(table).limit(limit).offset(offset)).fetchall()
    return jsonify({
        'data': [dict(r) for r in rows],
        'meta': {'page': page, 'limit': limit, 'total': total}
    })

# CRUD básicos (solo GET)
@app.route('/api/v1/usuarios', methods=['GET'])
def list_usuarios(): return paginate(Usuario)

@app.route('/api/v1/usuarios/<int:id>', methods=['GET'])
def get_usuario(id):
    with engine.connect() as conn:
        row = conn.execute(select(Usuario).where(Usuario.c.id_usuario == id)).first()
    return (jsonify({'data': dict(row)}) if row else ('', 404))

@app.route('/api/v1/clientes', methods=['GET'])
def list_clientes(): return paginate(Cliente)
@app.route('/api/v1/clientes/<int:id>', methods=['GET'])
def get_cliente(id):
    with engine.connect() as conn:
        row = conn.execute(select(Cliente).where(Cliente.c.id_usuario == id)).first()
    return (jsonify({'data': dict(row)}) if row else ('', 404))

@app.route('/api/v1/trabajadores', methods=['GET'])
def list_trabajadores(): return paginate(Trabajador)
@app.route('/api/v1/trabajadores/<int:id>', methods=['GET'])
def get_trabajador(id):
    with engine.connect() as conn:
        row = conn.execute(select(Trabajador).where(Trabajador.c.id_usuario == id)).first()
    return (jsonify({'data': dict(row)}) if row else ('', 404))

@app.route('/api/v1/administradores', methods=['GET'])
def list_administradores(): return paginate(Administrador)
@app.route('/api/v1/administradores/<int:id>', methods=['GET'])
def get_administrador(id):
    with engine.connect() as conn:
        row = conn.execute(select(Administrador).where(Administrador.c.id_usuario == id)).first()
    return (jsonify({'data': dict(row)}) if row else ('', 404))

@app.route('/api/v1/platos', methods=['GET'])
def list_platos(): return paginate(Plato)
@app.route('/api/v1/platos/<int:id>', methods=['GET'])
def get_plato(id):
    with engine.connect() as conn:
        row = conn.execute(select(Plato).where(Plato.c.id_plato == id)).first()
    return (jsonify({'data': dict(row)}) if row else ('', 404))

@app.route('/api/v1/menus', methods=['GET'])
def list_menus(): return paginate(Menu)
@app.route('/api/v1/menus/<int:id>', methods=['GET'])
def get_menu(id):
    with engine.connect() as conn:
        row = conn.execute(select(Menu).where(Menu.c.id_menu == id)).first()
    return (jsonify({'data': dict(row)}) if row else ('', 404))

@app.route('/api/v1/pedidos', methods=['GET'])
def list_pedidos(): return paginate(Pedido)
@app.route('/api/v1/pedidos/<int:id>', methods=['GET'])
def get_pedido(id):
    with engine.connect() as conn:
        row = conn.execute(select(Pedido).where(Pedido.c.id_pedido == id)).first()
    return (jsonify({'data': dict(row)}) if row else ('', 404))

@app.route('/api/v1/zonas', methods=['GET'])
def list_zonas(): return paginate(ZonaEntrega)
@app.route('/api/v1/zonas/<string:nombre>', methods=['GET'])
def get_zona(nombre):
    with engine.connect() as conn:
        row = conn.execute(select(ZonaEntrega).where(ZonaEntrega.c.nombre == nombre)).first()
    return (jsonify({'data': dict(row)}) if row else ('', 404))

# Endpoints de dashboard (consultas estrella)
@app.route('/api/v1/dashboard/platos-populares', methods=['GET'])
def platos_populares():
    sql = text("""
    SELECT 
        p.nombre AS nombre_plato,
        p.categoria,
        p.precio,
        u.nombre || ' ' || u.apellido AS administrador_creador,
        pd.zona_entrega,
        COUNT(DISTINCT pd.id_pedido) AS total_pedidos,
        ROUND(AVG(h.calificacion::numeric), 2) AS calificacion_promedio,
        COUNT(h.calificacion) AS total_calificaciones,
        SUM(p.precio) AS ingresos_generados
    FROM Plato p
    JOIN Pertenece pe ON p.id_plato = pe.id_plato
    JOIN Menu m ON pe.id_menu = m.id_menu
    JOIN Administrador a ON m.id_administrador = a.id_usuario
    JOIN Usuario u ON a.id_usuario = u.id_usuario
    JOIN Tiene t ON m.id_menu = t.id_menu
    JOIN Pedido pd ON t.id_pedido = pd.id_pedido
    LEFT JOIN Hace h ON pd.id_pedido = h.id_pedido
    WHERE pd.fecha >= CURRENT_DATE - INTERVAL '30 days'
      AND pd.estado = 'Entregado'
    GROUP BY p.id_plato, p.nombre, p.categoria, p.precio, 
             u.nombre, u.apellido, pd.zona_entrega
    HAVING COUNT(DISTINCT pd.id_pedido) >= 5
    ORDER BY total_pedidos DESC, calificacion_promedio DESC
    LIMIT 15;
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/v1/dashboard/rendimiento-zonas', methods=['GET'])
def rendimiento_zonas():
    sql = text("""
    SELECT 
        pd.zona_entrega,
        ze.costo AS costo_zona,
        COUNT(pd.id_pedido) AS total_entregas,
        COUNT(CASE WHEN pd.estado = 'Entregado' THEN 1 END) AS entregas_exitosas,
        ROUND(
            COUNT(CASE WHEN pd.estado = 'Entregado' THEN 1 END)::numeric / 
            COUNT(pd.id_pedido)::numeric * 100, 2
        ) AS porcentaje_exito,
        ROUND(AVG(
            EXTRACT(EPOCH FROM (pd.hora_entrega - pd.hora_salida)) / 60
        ), 2) AS tiempo_promedio_minutos,
        ROUND(AVG(
            EXTRACT(EPOCH FROM (pd.hora_entrega - pd.hora_entrega_estimada)) / 60
        ), 2) AS diferencia_estimado_real,
        COUNT(DISTINCT c.id_usuario) AS repartidores_activos,
        STRING_AGG(DISTDistinct u.nombre || ' ' || u.apellido, ', ') AS nombres_repartidores
    FROM Pedido pd
    JOIN ZonaEntrega ze ON pd.zona_entrega = ze.nombre
    JOIN Cubre c ON pd.zona_entrega = c.zona_entrega
    JOIN Usuario u ON c.id_usuario = u.id_usuario
    WHERE pd.fecha >= CURRENT_DATE - INTERVAL '30 days'
      AND pd.hora_salida IS NOT NULL
      AND pd.hora_entrega IS NOT NULL
      AND pd.hora_entrega_estimada IS NOT NULL
    GROUP BY pd.zona_entrega, ze.costo
    HAVING COUNT(pd.id_pedido) >= 5
    ORDER BY porcentaje_exito DESC, tiempo_promedio_minutos ASC;
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/v1/dashboard/top-repartidores', methods=['GET'])
def top_repartidores():
    sql = text("""
    SELECT 
        u.nombre || ' ' || u.apellido AS nombre_repartidor,
        t.telefono_emergencia,
        c.zona_entrega,
        COUNT(pd.id_pedido) AS entregas_realizadas,
        COUNT(CASE WHEN pd.estado = 'Entregado' THEN 1 END) AS entregas_exitosas,
        ROUND(
            COUNT(CASE WHEN pd.estado = 'Entregado' THEN 1 END)::numeric / 
            COUNT(pd.id_pedido)::numeric * 100, 2
        ) AS tasa_exito,
        ROUND(AVG(h.calificacion::numeric), 2) AS calificacion_promedio,
        ROUND(AVG(
            EXTRACT(EPOCH FROM (pd.hora_entrega - pd.hora_salida)) / 60
        ), 2) AS tiempo_promedio_entrega,
        COUNT(DISTINCT DATE(pd.fecha)) AS dias_trabajados,
        ROW_NUMBER() OVER (
            PARTITION BY c.zona_entrega 
            ORDER BY COUNT(CASE WHEN pd.estado = 'Entregado' THEN 1 END) DESC,
                     AVG(h.calificacion::numeric) DESC
        ) AS ranking_zona
    FROM Usuario u
    JOIN Trabajador t ON u.id_usuario = t.id_usuario
    JOIN Repartidor r ON t.id_usuario = r.id_usuario
    JOIN Cubre c ON r.id_usuario = c.id_usuario
    JOIN Pedido pd ON pd.zona_entrega = c.zona_entrega
    LEFT JOIN Hace h ON pd.id_pedido = h.id_pedido
    WHERE pd.estado IN ('Entregado', 'En reparto')
      AND pd.fecha >= CURRENT_DATE - INTERVAL '30 days'
      AND pd.hora_salida IS NOT NULL
      AND pd.hora_entrega IS NOT NULL
    GROUP BY u.id_usuario, u.nombre, u.apellido, t.telefono_emergencia, c.zona_entrega
    HAVING COUNT(pd.id_pedido) >= 3
    ORDER BY c.zona_entrega, ranking_zona;
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/v1/dashboard/clientes-activos', methods=['GET'])
def clientes_activos():
    sql = text("""
    SELECT 
        u.nombre || ' ' || u.apellido AS nombre_cliente,
        cl.empresa,
        v.zona_entrega,
        COUNT(pd.id_pedido) AS total_pedidos,
        ROUND(AVG(p.precio), 2) AS ticket_promedio,
        SUM(p.precio) AS valor_total_consumido,
        COUNT(DISTINCT pe.id_plato) AS variedad_platos_consumidos,
        COUNT(DISTINCT DATE(pd.fecha)) AS dias_activos,
        ROUND(AVG(h.calificacion::numeric), 2) AS calificacion_promedio,
        MAX(pd.fecha) AS ultimo_pedido,
        STRING_AGG(DISTINCT p.categoria, ', ') AS categorias_preferidas,
        CASE 
            WHEN COUNT(pd.id_pedido) >= 20 THEN 'Cliente VIP'
            WHEN COUNT(pd.id_pedido) >= 10 THEN 'Cliente Frecuente'
            WHEN COUNT(pd.id_pedido) >= 5 THEN 'Cliente Regular'
            ELSE 'Cliente Ocasional'
        END AS categoria_fidelidad,
        EXTRACT(DAYS FROM (CURRENT_DATE - MAX(pd.fecha))) AS dias_sin_pedido
    FROM Usuario u
    JOIN Cliente cl ON u.id_usuario = cl.id_usuario
    JOIN Vive v ON u.id_usuario = v.id_usuario
    JOIN Hace ha ON u.id_usuario = ha.id_usuario
    JOIN Pedido pd ON ha.id_pedido = pd.id_pedido
    JOIN Tiene t ON pd.id_pedido = t.id_pedido
    JOIN Menu m ON t.id_menu = m.id_menu
    JOIN Pertenece pe ON m.id_menu = pe.id_menu
    JOIN Plato p ON pe.id_plato = p.id_plato
    LEFT JOIN Hace h ON pd.id_pedido = h.id_pedido
    WHERE pd.fecha >= CURRENT_DATE - INTERVAL '60 days'
      AND pd.estado = 'Entregado'
    GROUP BY u.id_usuario, u.nombre, u.apellido, cl.empresa, v.zona_entrega
    HAVING COUNT(pd.id_pedido) >= 3
    ORDER BY total_pedidos DESC, valor_total_consumido DESC
    LIMIT 20;
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return jsonify([dict(r) for r in rows])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
