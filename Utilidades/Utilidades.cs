using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using DotNetDBF;
using Migracion.Clases;
using Npgsql;
using static Migracion.Utilidades.Utilidades;

namespace Migracion.Utilidades
{
    public class Utilidades
    {

        //public async Task<(int id, string codigo, string nombre)?> ObtenerSucursalAsync(
        //    NpgsqlConnection conn, string codigoSucursal)
        //{
        //    var sql = @"SELECT id, codigo, nombre 
        //        FROM ""Sucursales"" 
        //        WHERE codigo = @codigo 
        //        LIMIT 1;";

        //    using (var cmd = new NpgsqlCommand(sql, conn))
        //    {
        //        cmd.Parameters.AddWithValue("codigo", codigoSucursal);

        //        using (var reader = await cmd.ExecuteReaderAsync())
        //        {
        //            if (await reader.ReadAsync())
        //            {
        //                int id = reader.GetInt32(0);
        //                string codigo = reader.GetString(1);
        //                string nombre = reader.GetString(2);

        //                return (id, codigo, nombre);
        //            }
        //        }
        //    }

        //    return null; 

        //}



        public class RegistroPpe
        {

            public string CatPpe { get; set; }
            public string scat_ppe { get; set; }
            public string sbcat_ppe { get; set; }
            public string TipoElem { get; set; }
            public string Ref1Ppe { get; set; }
            public string Ref2Ppe { get; set; }
            public string ModcPpe { get; set; }
            public string CodAgrup { get; set; }
        }

        public class RegistroCabdesp
        {
            public string Despacho { get; set; }
            public string Ruta { get; set; }
            public string Conductor { get; set; }
            public string Observac { get; set; }
            public DateTime? Fech_Sal { get; set; }
            public string Placa { get; set; }
            public string TipProce { get; set; } // <- este campo faltaba
        }

        public class RegistroMovdesp
        {
            public string Despacho { get; set; }
            public int? Cant_Reg { get; set; }
        }


        public class ResultadoCabMov
        {
            public string Despacho { get; set; }
            public string Ruta { get; set; }
            public int? CantReg { get; set; }
            public string Conductor { get; set; }
            public string Observac { get; set; }
            public DateTime? Fech_Sal { get; set; }
            public string Placa { get; set; }
        }


        public class RegistroModc
        {
            public string ModcPpe { get; set; }
            public string CtaDepre { get; set; }
        }


        public class ResultadoPpeModc
        {
            public string CatPpe { get; set; }
            public string scat_ppe { get; set; }
            public string sbcat_ppe { get; set; }
            public string TipoElem { get; set; }
            public string Ref1Ppe { get; set; }
            public string Ref2Ppe { get; set; }
            public string ModcPpe { get; set; }
            public string CtaDepre { get; set; }
            public string CodAgrup { get; set; }
        }


        public static List<ResultadoCabMov> LeerYUnirTablas2()
        {

            string rutaBase = @"J:\sistemas\migracion";

            // --- 1) Leer cabdesp ---
            var listaCabdesp = new List<RegistroCabdesp>();
            string cabPath = Path.Combine(rutaBase, "cabdesp.dbf");

            using (var fs = File.Open(cabPath, FileMode.Open, FileAccess.Read))
            using (var reader = new DBFReader(fs))
            {
                reader.CharEncoding = System.Text.Encoding.GetEncoding(1252); // ANSI

                var fieldIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < reader.Fields.Length; i++)
                    fieldIndex[reader.Fields[i].Name] = i;

                object[] registro;
                int contador = 0;

                while ((registro = reader.NextRecord()) != null)
                {
                    string GetString(string fieldName)
                    {
                        if (!fieldIndex.TryGetValue(fieldName, out int idx)) return null;
                        var val = registro.Length > idx ? registro[idx] : null;
                        return val?.ToString()?.Trim();
                    }

                    DateTime? GetDatetime(string fieldName)
                    {
                        if (!fieldIndex.TryGetValue(fieldName, out int idx)) return null;
                        var val = registro.Length > idx ? registro[idx] : null;
                        if (val == null) return null;
                        if (DateTime.TryParse(val.ToString(), out var fecha)) return fecha;
                        return null;
                    }

                    var reg2 = new RegistroCabdesp
                    {
                        Despacho = GetString("despacho"),
                        Ruta = GetString("ruta"),
                        Conductor = GetString("conductor"),
                        Observac = GetString("observac"),
                        Fech_Sal = GetDatetime("fech_sal"),
                        Placa = GetString("placa")
                    };

                    listaCabdesp.Add(reg2);
                    contador++;
                }

                Console.WriteLine($"Tabla CABDESP leída: {contador} registros.");
            }

            // --- 2) Leer movdesp ---
            var listaMovdesp = new List<RegistroMovdesp>();
            string movPath = Path.Combine(rutaBase, "movdesp.dbf");

            using (var fs = File.Open(movPath, FileMode.Open, FileAccess.Read))
            using (var reader = new DBFReader(fs))
            {
                reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                var fieldIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < reader.Fields.Length; i++)
                    fieldIndex[reader.Fields[i].Name] = i;

                object[] registro = null;
                int contador = 0;

                while (true)
                {
                    try
                    {
                        registro = reader.NextRecord(); // ❗ Aquí es donde ocurre la excepción real

                        if (registro == null)
                            break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("\n🔥 ERROR LEYENDO REGISTRO DEL DBF 🔥");
                        Console.WriteLine($"Registro Nº: {contador + 1}");
                        Console.WriteLine("Campo dañado: desconocido (DotNetDBF falló antes de entregar datos).");
                        Console.WriteLine($"Mensaje: {ex.Message}");

                        throw; // vuelve a lanzar para ver stack si quieres
                    }

                    // ──────────────────────────────────────────────
                    // Después de este punto YA ES SEGURO PARSEAR
                    // ──────────────────────────────────────────────

                    string GetString(string fieldName)
                    {
                        if (!fieldIndex.TryGetValue(fieldName, out int idx)) return null;
                        var val = registro.Length > idx ? registro[idx] : null;
                        return val?.ToString()?.Trim();
                    }

                    int? GetInt(string fieldName)
                    {
                        if (!fieldIndex.TryGetValue(fieldName, out int idx))
                            return null;

                        if (registro.Length <= idx)
                            return null;

                        var val = registro[idx];
                        if (val == null)
                            return null;

                        if (val is double d) return (int)Math.Round(d);
                        if (val is float f) return (int)Math.Round(f);
                        if (val is decimal dec) return (int)Math.Round(dec);

                        string texto = val.ToString();
                        if (string.IsNullOrWhiteSpace(texto))
                            return null;

                        texto = System.Text.RegularExpressions.Regex.Replace(texto, @"[^\d\-\.,]", "")
                            .Replace(",", ".");

                        if (decimal.TryParse(texto, System.Globalization.NumberStyles.Any,
                            System.Globalization.CultureInfo.InvariantCulture, out var num))
                        {
                            return (int)Math.Round(num);
                        }

                        Console.WriteLine($"⚠ No se pudo convertir el número del campo {fieldName}: '{val}'");
                        return null;
                    }

                    var reg = new RegistroMovdesp
                    {
                        Despacho = GetString("despacho"),
                        Cant_Reg = GetInt("cant_reg")
                    };

                    listaMovdesp.Add(reg);
                    contador++;
                }

                Console.WriteLine($"Tabla MOVDESP leída: {contador} registros.");
            }




            //var resultado = (from c in listaCabdesp
            //                 join m in listaMovdesp
            //                 on c.Despacho equals m.Despacho
            //                 select new ResultadoCabMov
            //                 {
            //                     Despacho = c.Despacho,
            //                     Ruta = c.Ruta,
            //                     CantReg = Sum( m.Cant_Reg),
            //                     Conductor = c.Conductor,
            //                     Observac = c.Observac,
            //                     Fech_Sal = c.Fech_Sal,
            //                     Placa = c.Placa
            //                 }).ToList();

            var resultado = (from c in listaCabdesp
                             join m in listaMovdesp
                             on c.Despacho equals m.Despacho
                             group new { c, m } by c.Despacho into g
                             select new ResultadoCabMov
                             {
                                 Despacho = g.Key,
                                 Ruta = g.First().c.Ruta,
                                 CantReg = g.Sum(x => x.m.Cant_Reg),
                                 Conductor = g.First().c.Conductor,
                                 Observac = g.First().c.Observac,
                                 Fech_Sal = g.First().c.Fech_Sal,
                                 Placa = g.First().c.Placa
                             }).ToList();




            // --- 4) Mostrar algunos resultados ---
            Console.WriteLine("\n=== Resultados del JOIN (primeras 50 filas) ===");
            foreach (var fila in resultado.Take(50))
            {
                Console.WriteLine($"{fila.Despacho} | {fila.Ruta} | {fila.CantReg} | {fila.Conductor} | {fila.Placa}");
            }

            Console.WriteLine($"\nTotal combinaciones: {resultado.Count()}");

            return resultado;
        }

        //////////////////////////////////////////////////////////////////////////////////////////////////
        ///
        public static List<ResultadoPpeModc> LeerYUnirTablas()
        {
            string rutaBase = @"C:\tmp_archivos\";

            // --- 1) Leer tabla PPE ---
            var listaPpe = new List<RegistroPpe>();
            string ppePath = Path.Combine(rutaBase, "ppe.dbf");
            using (var fs = File.Open(ppePath, FileMode.Open, FileAccess.Read))
            using (var reader = new DBFReader(fs))
            {
                reader.CharEncoding = System.Text.Encoding.GetEncoding(1252); // ANSI

                // Construir mapa nombre->índice (insensible a mayúsculas)
                var fieldIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < reader.Fields.Length; i++)
                    fieldIndex[reader.Fields[i].Name] = i;

                object[] registro;
                int contador = 0;
                while ((registro = reader.NextRecord()) != null)
                {
                    // Función local para obtener valor seguro por nombre
                    string GetString(string fieldName)
                    {
                        if (fieldName == null) return null;
                        if (!fieldIndex.TryGetValue(fieldName, out int idx)) return null;
                        var val = registro.Length > idx ? registro[idx] : null;
                        return val?.ToString()?.Trim();
                    }

                    var reg = new RegistroPpe
                    {
                        CatPpe = GetString("cat_ppe"),
                        scat_ppe = GetString("scat_ppe"),
                        sbcat_ppe = GetString("sbcat_ppe"),
                        TipoElem = GetString("tipo_elem"),
                        Ref1Ppe = GetString("ref1_ppe"),
                        Ref2Ppe = GetString("ref2_ppe"),
                        ModcPpe = GetString("modc_ppe"),
                        CodAgrup = GetString("cod_agrup")
                    };
                    listaPpe.Add(reg);
                    contador++;
                }

                Console.WriteLine($"Tabla PPE leida: {contador} registros.");
            }

            // --- 2) Leer tabla MODC_PPE ---
            var listaModc = new List<RegistroModc>();
            string modcPath = Path.Combine(rutaBase, "modc_ppe.dbf");
            using (var fs = File.Open(modcPath, FileMode.Open, FileAccess.Read))
            using (var reader = new DBFReader(fs))
            {
                reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                var fieldIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < reader.Fields.Length; i++)
                    fieldIndex[reader.Fields[i].Name] = i;

                object[] registro;
                int contador = 0;
                while ((registro = reader.NextRecord()) != null)
                {
                    string GetString(string fieldName)
                    {
                        if (fieldName == null) return null;
                        if (!fieldIndex.TryGetValue(fieldName, out int idx)) return null;
                        var val = registro.Length > idx ? registro[idx] : null;
                        return val?.ToString()?.Trim();
                    }

                    var reg = new RegistroModc
                    {
                        ModcPpe = GetString("modc_ppe"),
                        CtaDepre = GetString("cta_deprec")
                    };
                    listaModc.Add(reg);
                    contador++;
                }

                Console.WriteLine($"Tabla MODC_PPE leida: {contador} registros.");
            }


            var resultado = (from p in listaPpe
                             join m in listaModc
                             on p.ModcPpe equals m.ModcPpe
                             select new ResultadoPpeModc
                             {
                                 CatPpe = p.CatPpe,
                                 scat_ppe = p.scat_ppe,
                                 sbcat_ppe = p.sbcat_ppe,
                                 TipoElem = p.TipoElem,
                                 Ref1Ppe = p.Ref1Ppe,
                                 Ref2Ppe = p.Ref2Ppe,
                                 ModcPpe = m.ModcPpe,
                                 CtaDepre = m.CtaDepre,
                                 CodAgrup = p.CodAgrup
                             }).ToList();

            // --- 4) Mostrar algunos resultados ---
            Console.WriteLine("\n=== Resultados del JOIN (primeras 50 filas) ===");
            foreach (var fila in resultado.Take(50))
            {
                Console.WriteLine($"{fila.CatPpe} | {fila.TipoElem} | {fila.ModcPpe} | {fila.CtaDepre}");
            }

            Console.WriteLine($"\nTotal combinaciones: {resultado.Count()}");

            return resultado;
        }



        //public static List<ResultadoPpeModc> ListMovd()
        //{
        //    string rutaBase = @"C:\tmp_archivos\";



        //    string rutaRed = @"j:\businsas\mersas\datos\docum.dbf";
        //    string rutaRed = @"j:\businsas\mersas\datos\usuarios.dbf";
        //    string rutaLocal = @"C:\MisDBF\archivo.dbf";

        //    using (var origen = new FileStream(rutaRed, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        //    using (var destino = new FileStream(rutaLocal, FileMode.Create, FileAccess.Write))
        //    {
        //        origen.CopyTo(destino);
        //    }

        //    Console.WriteLine("Archivo DBF copiado con éxito.");



        //    // --- 1) Leer tabla PPE ---
        //    var listaPpe = new List<RegistroPpe>();
        //    string ppePath = Path.Combine(rutaBase, "ppe.dbf");
        //    using (var fs = File.Open(ppePath, FileMode.Open, FileAccess.Read))
        //    using (var reader = new DBFReader(fs))
        //    {
        //        reader.CharEncoding = System.Text.Encoding.GetEncoding(1252); // ANSI

        //        // Construir mapa nombre->índice (insensible a mayúsculas)
        //        var fieldIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        //        for (int i = 0; i < reader.Fields.Length; i++)
        //            fieldIndex[reader.Fields[i].Name] = i;

        //        object[] registro;
        //        int contador = 0;
        //        while ((registro = reader.NextRecord()) != null)
        //        {
        //            // Función local para obtener valor seguro por nombre
        //            string GetString(string fieldName)
        //            {
        //                if (fieldName == null) return null;
        //                if (!fieldIndex.TryGetValue(fieldName, out int idx)) return null;
        //                var val = registro.Length > idx ? registro[idx] : null;
        //                return val?.ToString()?.Trim();
        //            }

        //            var reg = new RegistroPpe
        //            {
        //                CatPpe = GetString("cat_ppe"),
        //                scat_ppe = GetString("scat_ppe"),
        //                sbcat_ppe = GetString("sbcat_ppe"),
        //                TipoElem = GetString("tipo_elem"),
        //                Ref1Ppe = GetString("ref1_ppe"),
        //                Ref2Ppe = GetString("ref2_ppe"),
        //                ModcPpe = GetString("modc_ppe"),
        //                CodAgrup = GetString("cod_agrup")
        //            };
        //            listaPpe.Add(reg);
        //            contador++;
        //        }

        //        Console.WriteLine($"Tabla PPE leida: {contador} registros.");
        //    }

        //    // --- 2) Leer tabla MODC_PPE ---
        //    var listaModc = new List<RegistroModc>();
        //    string modcPath = Path.Combine(rutaBase, "modc_ppe.dbf");
        //    using (var fs = File.Open(modcPath, FileMode.Open, FileAccess.Read))
        //    using (var reader = new DBFReader(fs))
        //    {
        //        reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

        //        var fieldIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        //        for (int i = 0; i < reader.Fields.Length; i++)
        //            fieldIndex[reader.Fields[i].Name] = i;

        //        object[] registro;
        //        int contador = 0;
        //        while ((registro = reader.NextRecord()) != null)
        //        {
        //            string GetString(string fieldName)
        //            {
        //                if (fieldName == null) return null;
        //                if (!fieldIndex.TryGetValue(fieldName, out int idx)) return null;
        //                var val = registro.Length > idx ? registro[idx] : null;
        //                return val?.ToString()?.Trim();
        //            }

        //            var reg = new RegistroModc
        //            {
        //                ModcPpe = GetString("modc_ppe"),
        //                CtaDepre = GetString("cta_deprec")
        //            };
        //            listaModc.Add(reg);
        //            contador++;
        //        }

        //        Console.WriteLine($"Tabla MODC_PPE leida: {contador} registros.");
        //    }


        //    var resultado = (from p in listaPpe
        //                     join m in listaModc
        //                     on p.ModcPpe equals m.ModcPpe
        //                     select new ResultadoPpeModc
        //                     {
        //                         CatPpe = p.CatPpe,
        //                         scat_ppe = p.scat_ppe,
        //                         sbcat_ppe = p.sbcat_ppe,
        //                         TipoElem = p.TipoElem,
        //                         Ref1Ppe = p.Ref1Ppe,
        //                         Ref2Ppe = p.Ref2Ppe,
        //                         ModcPpe = m.ModcPpe,
        //                         CtaDepre = m.CtaDepre,
        //                         CodAgrup = p.CodAgrup
        //                     }).ToList();

        //    // --- 4) Mostrar algunos resultados ---
        //    Console.WriteLine("\n=== Resultados del JOIN (primeras 50 filas) ===");
        //    foreach (var fila in resultado.Take(50))
        //    {
        //        Console.WriteLine($"{fila.CatPpe} | {fila.TipoElem} | {fila.ModcPpe} | {fila.CtaDepre}");
        //    }

        //    Console.WriteLine($"\nTotal combinaciones: {resultado.Count()}");

        //    return resultado;
        //}



        //public async static Task<List<Resultado_sepa>> listPostgrest_separados()
        //{
        //    var listaResultados = new List<Resultado_sepa>();

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Include Error Detail=true"))
        //    {
        //        if (conn.State != System.Data.ConnectionState.Open)
        //            await conn.OpenAsync();

        //        using (var cmd = new NpgsqlCommand(@"SELECT id, cajasmovimientosid FROM public.separadomercancia", conn))
        //        using (var reader = await cmd.ExecuteReaderAsync())
        //        {
        //            while (await reader.ReadAsync())
        //            {
        //                var resultado = new Resultado_sepa
        //                {
        //                    Id = reader.GetInt32(0),
        //                    CajasMovimientosId = reader.GetInt32(1)
        //                };

        //                listaResultados.Add(resultado);
        //            }
        //        }
        //    }

        //    return listaResultados;
        //}





        public async static Task<List<Resultado_detail>> listPostgrest_cajasDetalles()
        {
            var listaResultados = new List<Resultado_detail>();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Include Error Detail=true"))
            {
                if (conn.State != System.Data.ConnectionState.Open)
                    await conn.OpenAsync();

                using (var cmd = new NpgsqlCommand(@"SELECT productoid, cajasmovimientosid, id FROM public.cajasmovimientosgenerales", conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var resultado = new Resultado_detail
                        {
                            productoid = reader.GetInt32(0),               // productoid
                            CajasMovimientosId = reader.GetInt32(1),      // cajasmovimientosid
                            Id = reader.GetInt32(2)                        // id
                        };

                        listaResultados.Add(resultado);
                    }
                }
            }

            return listaResultados;
        }


        public async static Task<List<Resultado_productos>> listPostgrest_productos()
        {
            var listaResultados = new List<Resultado_productos>();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Include Error Detail=true"))
            {
                if (conn.State != System.Data.ConnectionState.Open)
                    await conn.OpenAsync();

                using (var cmd = new NpgsqlCommand(@"SELECT ""Nombre"", ""Id"" FROM public.""Productos""", conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var resultado = new Resultado_productos
                        {
                            Nombre = reader.GetString(0),               // productoid
                            Id = reader.GetInt32(1)                        // id
                        };

                        listaResultados.Add(resultado);
                    }
                }
            }

            return listaResultados;
        }


        public async static Task<List<ResultadoJoin>> ListarSeparadoJoinAsync()
        {
            var lista = new List<ResultadoJoin>();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Include Error Detail=true"))
            {
                await conn.OpenAsync();

                string query = @"
                SELECT 
                    s.id AS separadomercanciaid, 
                    s.cajasmovimientosid, 
                    c.productoid, 
                    p.""Nombre"" AS nombreproducto
                FROM public.separadomercancia AS s
                INNER JOIN public.cajasmovimientosgenerales AS c  
                    ON s.cajasmovimientosid = c.cajasmovimientosid
                INNER JOIN public.""Productos"" AS p 
                    ON c.productoid = p.""Id"";";

                using (var cmd = new NpgsqlCommand(query, conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        lista.Add(new ResultadoJoin
                        {
                            separadomercanciaid = reader.GetInt32(0),
                            cajasmovimientosid = reader.GetInt32(1),
                            productoid = reader.GetInt32(2),
                            nombreproducto = reader.GetString(3)
                        });
                    }
                }
            }

            return lista;
        }



        public async static Task<List<Resultado_joinOrdenCompra>> ListarOrdenSucursalJoinAsync()
        {
            var lista = new List<Resultado_joinOrdenCompra>();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Include Error Detail=true"))
            {
                await conn.OpenAsync();

                string query = @"
                SELECT
                    o.id as ordenesdecompraid,
                    s.id as sucursalid
                FROM public.ordenesdecompra AS o
                INNER JOIN public.""Sucursales"" AS s
                    ON o.idsucursal = s.id;";

                using (var cmd = new NpgsqlCommand(query, conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        lista.Add(new Resultado_joinOrdenCompra
                        {
                            ordenesdecompraid = reader.GetInt32(0),
                            sucursalid = reader.GetInt32(1),

                        });
                    }
                }
            }

            return lista;
        }

        //public static void CursorFox (string ruta)
        //{
        //    string connectionString = $@"
        //    Provider=VFPOLEDB.1;
        //    Data Source={ruta};
        //    Collating Sequence=machine;
        //    ";

        //    using (var conexion = new OleDbConnection(connectionString))
        //    {
        //        conexion.Open();

        //        // Ejemplo: unir dos tablas DBF
        //        string sql = @"
        //        SELECT 
        //        p.cat_ppe, ;
        //        p.tipo_elem, ;
        //        p.ref1_ppe, ;
        //        p.ref2_ppe, ;
        //        m.modc_ppe, ;
        //        m.cta_gasto, ;
        //        p.cod_agrup;
        //        FROM  ppe p
        //        INNER JOIN modc_ppe m ON p.modc_ppe = m.modc_ppe;
        //        group by p.cod_agrup;
        //    ";


        //        using (var cmd = new OleDbCommand(sql, conexion))
        //        using (var reader = cmd.ExecuteReader())
        //        {
        //            while (reader.Read())
        //            {
        //                Console.WriteLine($"{reader["Codigo"]} - {reader["Descripcion"]} - {reader["Existencia"]} - {reader["Precio"]}");
        //            }
        //        }
        //    }



        //}




    }
}
