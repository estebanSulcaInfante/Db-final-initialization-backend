import psycopg2
from faker import Faker
from faker_food import FoodProvider
from random import randint, choice, sample
import sys
import time
import os

# Inicializar Faker con proveedor de comida
fake = Faker()
fake.add_provider(FoodProvider)


# Obtener la URL de la base de datos de Heroku desde la variable de entorno
DATABASE_URL = os.environ.get('DATABASE_URL')
# Obtener el nombre de la base de datos desde la URL
database = DATABASE_URL.split('/')[-1]  # El nombre de la base de datos es la última parte de la URL

def connect_db():
    """Conectar a la base de datos usando la URL proporcionada por Heroku"""
    return psycopg2.connect(DATABASE_URL)


def print_progress(operation, current, total, start_time=None):
    """Imprime el progreso cada 10,000 registros"""
    if current % 10000 == 0 or current == total:
        if start_time:
            elapsed = time.time() - start_time
            rate = current / elapsed if elapsed > 0 else 0
            eta = (total - current) / rate if rate > 0 else 0
            print(f"[{operation}] {current:,}/{total:,} | {rate:.0f} reg/seg | Tiempo: {elapsed:.1f}s | ETA: {eta:.1f}s")
        else:
            print(f"[{operation}] Procesados: {current:,}/{total:,} registros")
        sys.stdout.flush()

def estimate_final_size(n):
    """Estima el tamaño final basado en 25k = 68MB"""
    ratio = n / 25000
    estimated_mb = 68 * ratio
    if estimated_mb < 1024:
        return f"{estimated_mb:.1f} MB"
    else:
        return f"{estimated_mb/1024:.2f} GB"

def create_usuario_batch(cursor, n, batch_size=5000):
    """Crea usuarios en lotes para mejor rendimiento"""
    print(f"[USUARIOS] Iniciando inserción de {n:,} usuarios en lotes de {batch_size:,}...")
    start_time = time.time()
    
    all_user_ids = []
    
    for batch_start in range(0, n, batch_size):
        batch_end = min(batch_start + batch_size, n)
        batch_count = batch_end - batch_start
        
        usuarios = []
        for _ in range(batch_count):
            nombre = fake.first_name()[:50]
            apellido = fake.last_name()[:50]
            telefono = fake.phone_number()[:20]
            usuarios.append((nombre, apellido, telefono))
        
        cursor.executemany(
            "INSERT INTO Usuario (nombre, apellido, numero_telef) VALUES (%s, %s, %s)", usuarios)
        
        # Obtener IDs del lote actual
        cursor.execute("SELECT id_usuario FROM Usuario ORDER BY id_usuario DESC LIMIT %s", (batch_count,))
        batch_ids = [row[0] for row in cursor.fetchall()][::-1]
        all_user_ids.extend(batch_ids)
        
        # Progreso
        print_progress("USUARIOS", batch_end, n, start_time)
    
    elapsed = time.time() - start_time
    print(f"[USUARIOS] ✅ Completado: {n:,} usuarios en {elapsed:.1f}s")
    return all_user_ids

def create_large_dataset(n):
    """Crea un dataset grande optimizado para 1M+ registros"""
    print("="*80)
    print("🍔 FREDYS FOOD - SEEDER MASIVO")
    print("="*80)
    
    estimated_size = estimate_final_size(n)
    estimated_time = (n / 25000) * 120  # Aproximadamente 2 minutos por cada 25k
    
    print(f"📊 Configuración del seeder masivo:")
    print(f"   • Usuarios base: {n:,}")
    print(f"   • Clientes: {n//2:,}")
    print(f"   • Trabajadores: {n//2:,}")
    print(f"   • Repartidores: {n//4:,}")
    print(f"   • Administradores: {n//8:,}")
    print(f"   • Menús: {n:,}")
    print(f"   • Platos: {n:,}")
    print(f"   • Pedidos: {n:,}")
    print(f"")
    print(f"📈 Estimaciones:")
    print(f"   • Tamaño final estimado: {estimated_size}")
    print(f"   • Tiempo estimado: {estimated_time/60:.1f} minutos")
    print(f"   • Registros totales: {n*6:,}")
    print("="*80)
    
    # Información para datasets grandes
    if n >= 100000:
        print(f"⚠️  DATASET MUY GRANDE: {n:,} registros base")
        print(f"   Este proceso puede tomar {estimated_time/60:.1f} minutos")
        print(f"   Tamaño estimado final: {estimated_size}")
        print(f"   Iniciando automáticamente...")
    
    # Verificar espacio en disco disponible sería buena idea aquí
    
    total_start_time = time.time()
    
    try:
        conn = connect_db()
        cur = conn.cursor()
        
        print(f"\n🚀 Iniciando seeder masivo...")
        
        # Verificar que la base está vacía y limpiar solo si es necesario
        cur.execute("SELECT COUNT(*) FROM Usuario;")
        existing_users = cur.fetchone()[0]
        
        if existing_users > 0:
            print(f"⚠️  Base de datos no está vacía ({existing_users:,} usuarios encontrados)")
            print("[LIMPIEZA] Limpiando tablas...")
            all_tables = ['Hace','Cubre','Vive','Tiene','Pedido','Pertenece','Plato','Menu','Administrador','Repartidor','Trabajador','Cliente','Usuario','ZonaEntrega']
            for table in all_tables:
                cur.execute(f"DELETE FROM {table} CASCADE")
            conn.commit()
            print("[LIMPIEZA] ✅ Completada")
        else:
            print("[VERIFICACIÓN] ✅ Base de datos está vacía, procediendo directamente...")
        
        # Crear datos en lotes
        user_ids = create_usuario_batch(cur, n)
        
        # Resto de datos en lotes también
        print(f"[CLIENTES] Creando {n//2:,} clientes...")
        start_time = time.time()
        cliente_sample = sample(user_ids, k=n//2)
        clientes = [(uid, fake.company()[:100]) for uid in cliente_sample]
        cur.executemany("INSERT INTO Cliente (id_usuario, empresa) VALUES (%s, %s)", clientes)
        print(f"[CLIENTES] ✅ Completado en {time.time() - start_time:.1f}s")
        
        # Continuar con el resto...
        print(f"[TRABAJADORES] Creando {n//2:,} trabajadores...")
        start_time = time.time()
        trab_sample = sample(user_ids, k=n//2)
        trabajadores = [(uid, fake.phone_number()[:30]) for uid in trab_sample]
        cur.executemany("INSERT INTO Trabajador (id_usuario, telefono_emergencia) VALUES (%s, %s)", trabajadores)
        print(f"[TRABAJADORES] ✅ Completado en {time.time() - start_time:.1f}s")
        
        conn.commit()
        print("[COMMIT] ✅ Usuarios y roles confirmados")
        
        # Obtener tamaño intermedio
        cur.execute(f"SELECT pg_size_pretty(pg_database_size('{database}'));")
        intermediate_size = cur.fetchone()[0]
        print(f"📊 Tamaño intermedio: {intermediate_size}")
        
        # Continuar con repartidores y administradores
        print(f"[REPARTIDORES] Creando {n//4:,} repartidores...")
        start_time = time.time()
        reparto_sample = sample(trab_sample, k=n//4)
        repartidores = [(uid,) for uid in reparto_sample]
        cur.executemany("INSERT INTO Repartidor (id_usuario) VALUES (%s)", repartidores)
        print(f"[REPARTIDORES] ✅ Completado en {time.time() - start_time:.1f}s")
        
        print(f"[ADMINISTRADORES] Creando {n//8:,} administradores...")
        start_time = time.time()
        admin_sample = sample(trab_sample, k=n//8)
        administradores = [(uid, fake.email()[:100]) for uid in admin_sample]
        cur.executemany("INSERT INTO Administrador (id_usuario, correo) VALUES (%s, %s)", administradores)
        print(f"[ADMINISTRADORES] ✅ Completado en {time.time() - start_time:.1f}s")
        
        conn.commit()
        print("[COMMIT] ✅ Roles completados")
        
        # Crear menús en lotes
        print(f"[MENÚS] Creando {n:,} menús...")
        start_time = time.time()
        batch_size = 10000
        menu_ids = []
        
        for batch_start in range(0, n, batch_size):
            batch_end = min(batch_start + batch_size, n)
            batch_count = batch_end - batch_start
            
            menus = []
            for _ in range(batch_count):
                id_admin = choice(admin_sample)
                variacion = fake.word()[:50]
                fecha = fake.date_between(start_date='-1y', end_date='today')
                menus.append((id_admin, variacion, fecha))
            
            cur.executemany("INSERT INTO Menu (id_administrador, variacion, fecha) VALUES (%s, %s, %s)", menus)
            
            # Obtener IDs del lote
            cur.execute("SELECT id_menu FROM Menu ORDER BY id_menu DESC LIMIT %s", (batch_count,))
            batch_ids = [row[0] for row in cur.fetchall()][::-1]
            menu_ids.extend(batch_ids)
            
            print_progress("MENÚS", batch_end, n, start_time)
        
        print(f"[MENÚS] ✅ Completado: {n:,} menús en {time.time() - start_time:.1f}s")
        
        # Crear platos en lotes
        print(f"[PLATOS] Creando {n:,} platos...")
        start_time = time.time()
        plato_ids = []
        
        for batch_start in range(0, n, batch_size):
            batch_end = min(batch_start + batch_size, n)
            batch_count = batch_end - batch_start
            
            platos = []
            for _ in range(batch_count):
                nombre = fake.dish()[:100]
                foto = fake.image_url()
                tipo = choice(['Entrante', 'Principal', 'Postre', 'Bebida'])[:30]
                categoria = choice(['Vegano', 'Vegetariano', 'Carne', 'Pescado', 'Sin Gluten'])[:30]
                precio = round(fake.pyfloat(left_digits=2, right_digits=2, positive=True, min_value=5.0, max_value=50.0), 2)
                cod_nutri = fake.uuid4()[:36]
                platos.append((nombre, foto, tipo, categoria, precio, cod_nutri))
            
            cur.executemany("INSERT INTO Plato (nombre, foto, tipo, categoria, precio, codigo_info_nutricional) VALUES (%s, %s, %s, %s, %s, %s)", platos)
            
            # Obtener IDs del lote
            cur.execute("SELECT id_plato FROM Plato ORDER BY id_plato DESC LIMIT %s", (batch_count,))
            batch_ids = [row[0] for row in cur.fetchall()][::-1]
            plato_ids.extend(batch_ids)
            
            print_progress("PLATOS", batch_end, n, start_time)
        
        print(f"[PLATOS] ✅ Completado: {n:,} platos en {time.time() - start_time:.1f}s")
        
        conn.commit()
        print("[COMMIT] ✅ Catálogo completado")
        
        # Zonas de entrega
        zonas = [('Centro', 5.00), ('Norte', 7.50), ('Sur', 6.50), ('Este', 8.00), ('Oeste', 7.00)]
        cur.executemany("INSERT INTO ZonaEntrega (nombre, costo) VALUES (%s, %s)", zonas)
        zona_nombres = [z[0] for z in zonas]
        print(f"[ZONAS] ✅ {len(zonas)} zonas insertadas")
        
        # Crear pedidos en lotes
        print(f"[PEDIDOS] Creando {n:,} pedidos...")
        start_time = time.time()
        pedido_ids = []
        
        for batch_start in range(0, n, batch_size):
            batch_end = min(batch_start + batch_size, n)
            batch_count = batch_end - batch_start
            
            pedidos = []
            for _ in range(batch_count):
                fecha = fake.date_time_between(start_date='-30d', end_date='now')
                estado = choice(['Pendiente', 'Enviado', 'Entregado', 'Cancelado'])
                hs, he, he_est = fake.time(), fake.time(), fake.time()
                direccion = fake.address()[:200]
                zona = choice(zona_nombres)
                id_cliente = choice(cliente_sample)
                pedidos.append((fecha, estado, hs, he, he_est, direccion, zona, id_cliente))
            
            cur.executemany("INSERT INTO Pedido (fecha, estado, hora_salida, hora_entrega, hora_entrega_estimada, direccion_exacta, zona_entrega, id_cliente) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", pedidos)
            
            # Obtener IDs del lote
            cur.execute("SELECT id_pedido FROM Pedido ORDER BY id_pedido DESC LIMIT %s", (batch_count,))
            batch_ids = [row[0] for row in cur.fetchall()][::-1]
            pedido_ids.extend(batch_ids)
            
            print_progress("PEDIDOS", batch_end, n, start_time)
        
        print(f"[PEDIDOS] ✅ Completado: {n:,} pedidos en {time.time() - start_time:.1f}s")
        
        conn.commit()
        print("[COMMIT] ✅ Pedidos completados")
        
        # Crear relaciones Menu-Plato (Pertenece)
        print(f"[MENU-PLATO] Creando relaciones...")
        start_time = time.time()
        relaciones_pertenece = []
        for mid in menu_ids:
            # Cada menú tiene 1-4 platos
            platos_seleccionados = sample(plato_ids, k=randint(1, min(4, len(plato_ids))))
            for pid in platos_seleccionados:
                relaciones_pertenece.append((mid, pid))
        
        # Insertar en lotes
        batch_size_rel = 50000
        for i in range(0, len(relaciones_pertenece), batch_size_rel):
            batch = relaciones_pertenece[i:i+batch_size_rel]
            cur.executemany("INSERT INTO Pertenece (id_menu, id_plato) VALUES (%s, %s)", batch)
            if i % 100000 == 0:
                print(f"[MENU-PLATO] Procesadas {i:,}/{len(relaciones_pertenece):,} relaciones")
        
        print(f"[MENU-PLATO] ✅ Completado: {len(relaciones_pertenece):,} relaciones en {time.time() - start_time:.1f}s")
        
        # Crear relaciones Pedido-Menu (Tiene)
        print(f"[PEDIDO-MENU] Creando relaciones...")
        start_time = time.time()
        relaciones_tiene = []
        for pid in pedido_ids:
            # Cada pedido tiene 1-3 menús
            menus_seleccionados = sample(menu_ids, k=randint(1, 3))
            for mid in menus_seleccionados:
                relaciones_tiene.append((pid, mid))
        
        # Insertar en lotes
        for i in range(0, len(relaciones_tiene), batch_size_rel):
            batch = relaciones_tiene[i:i+batch_size_rel]
            cur.executemany("INSERT INTO Tiene (id_pedido, id_menu) VALUES (%s, %s)", batch)
            if i % 100000 == 0:
                print(f"[PEDIDO-MENU] Procesadas {i:,}/{len(relaciones_tiene):,} relaciones")
        
        print(f"[PEDIDO-MENU] ✅ Completado: {len(relaciones_tiene):,} relaciones en {time.time() - start_time:.1f}s")
        
        # Crear calificaciones (Hace)
        print(f"[CALIFICACIONES] Creando {len(pedido_ids):,} calificaciones...")
        start_time = time.time()
        calificaciones = []
        for pid in pedido_ids:
            uid = choice(user_ids)
            calificacion = randint(1, 5)
            comentario = fake.text(max_nb_chars=100)
            calificaciones.append((pid, uid, calificacion, comentario))
        
        # Insertar en lotes
        for i in range(0, len(calificaciones), batch_size_rel):
            batch = calificaciones[i:i+batch_size_rel]
            cur.executemany("INSERT INTO Hace (id_pedido, id_usuario, calificacion, comentario) VALUES (%s,%s,%s,%s)", batch)
            if i % 100000 == 0:
                print(f"[CALIFICACIONES] Procesadas {i:,}/{len(calificaciones):,}")
        
        print(f"[CALIFICACIONES] ✅ Completado: {len(calificaciones):,} calificaciones en {time.time() - start_time:.1f}s")
        
        # Crear relaciones Usuario-Zona (Vive)
        print(f"[USUARIO-ZONA] Creando relaciones...")
        start_time = time.time()
        relaciones_vive = []
        for uid in user_ids:
            zona = choice(zona_nombres)
            relaciones_vive.append((zona, uid))
        
        # Insertar en lotes
        for i in range(0, len(relaciones_vive), batch_size_rel):
            batch = relaciones_vive[i:i+batch_size_rel]
            cur.executemany("INSERT INTO Vive (zona_entrega, id_usuario) VALUES (%s, %s)", batch)
            if i % 100000 == 0:
                print(f"[USUARIO-ZONA] Procesadas {i:,}/{len(relaciones_vive):,}")
        
        print(f"[USUARIO-ZONA] ✅ Completado: {len(relaciones_vive):,} relaciones en {time.time() - start_time:.1f}s")
        
        # Crear relaciones Repartidor-Zona (Cubre)
        print(f"[REPARTIDOR-ZONA] Creando relaciones...")
        start_time = time.time()
        relaciones_cubre = []
        for rid in reparto_sample:
            zona = choice(zona_nombres)
            relaciones_cubre.append((zona, rid))
        
        cur.executemany("INSERT INTO Cubre (zona_entrega, id_usuario) VALUES (%s,%s)", relaciones_cubre)
        print(f"[REPARTIDOR-ZONA] ✅ Completado: {len(relaciones_cubre):,} relaciones en {time.time() - start_time:.1f}s")
        
        # Commit final
        conn.commit()
        print("[COMMIT] ✅ Todas las relaciones completadas")
        
        # Obtener tamaño final
        cur.execute(f"SELECT pg_size_pretty(pg_database_size('{database}'));")
        final_size = cur.fetchone()[0]
        
        # Contar todos los registros para el resumen
        total_registros = (len(user_ids) + len(cliente_sample) + len(trab_sample) + 
                          len(reparto_sample) + len(admin_sample) + len(menu_ids) + 
                          len(plato_ids) + len(pedido_ids) + len(zonas) +
                          len(relaciones_pertenece) + len(relaciones_tiene) + 
                          len(calificaciones) + len(relaciones_vive) + len(relaciones_cubre))
        
        total_elapsed = time.time() - total_start_time
        print("="*80)
        print("🎉 SEEDER MASIVO COMPLETADO!")
        print("="*80)
        print(f"📊 Resumen final:")
        print(f"   • {len(user_ids):,} usuarios")
        print(f"   • {len(cliente_sample):,} clientes")
        print(f"   • {len(trab_sample):,} trabajadores")
        print(f"   • {len(reparto_sample):,} repartidores")
        print(f"   • {len(admin_sample):,} administradores")
        print(f"   • {len(menu_ids):,} menús")
        print(f"   • {len(plato_ids):,} platos")
        print(f"   • {len(pedido_ids):,} pedidos")
        print(f"   • {len(zonas)} zonas de entrega")
        print(f"   • {len(relaciones_pertenece):,} relaciones menú-plato")
        print(f"   • {len(relaciones_tiene):,} relaciones pedido-menú")
        print(f"   • {len(calificaciones):,} calificaciones")
        print(f"   • {len(relaciones_vive):,} relaciones usuario-zona")
        print(f"   • {len(relaciones_cubre):,} relaciones repartidor-zona")
        print(f"")
        print(f"📈 Estadísticas:")
        print(f"   • Total de registros: {total_registros:,}")
        print(f"   • Tiempo total: {total_elapsed/60:.1f} minutos ({total_elapsed:.1f} segundos)")
        print(f"   • Tamaño final: {final_size}")
        print(f"   • Velocidad promedio: {total_registros / total_elapsed:.0f} registros/segundo")
        print(f"")
        print(f"🎯 Base de datos lista para testing masivo!")
        print("="*80)
        
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        return False

def main():
    if len(sys.argv) != 2:
        print("❌ Uso: python seeder_massive.py <num_registros_base>")
        print("📝 Ejemplo: python seeder_massive.py 1000000")
        sys.exit(1)
    
    try:
        n = int(sys.argv[1])
    except ValueError:
        print("❌ Error: El argumento debe ser un número entero")
        sys.exit(1)
    
    if n <= 0:
        print("❌ Error: El número de registros debe ser mayor a 0")
        sys.exit(1)
    
    success = create_large_dataset(n)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
