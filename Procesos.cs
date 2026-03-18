using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Drawing;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Reflection.Emit;
using System.Runtime.Intrinsics.X86;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Transactions;
using System.Xml.Linq;
using Migracion.Clases;
using Migracion.Utilidades;
using Npgsql;
using NpgsqlTypes;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Migracion
{
     class Procesos
    {



        public static void InsertarVendedores(List<t_vended> vendedList)
        {
            var cronometer = Stopwatch.StartNew();
            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {

                
                conn.Open();


                // Iniciar transacción
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;


                    // 1. Ejecutar TRUNCATE con CASCADE antes de insertar
                    cmd.CommandText = "TRUNCATE TABLE vendedores CASCADE;";
                    cmd.ExecuteNonQuery();


                    // Insertar cada registro de vended
                    foreach (var vended in vendedList)
                    {
                        // Comando SQL con parámetros
                        cmd.CommandText = @"
                        INSERT INTO vendedores (codigo, nombre, telefonos, direccion, ciudad, numerodedocumento, fechahoraingreso, estado, sucursalid)
                        VALUES (@codigo, @nombre, @telefonos, @direccion, @ciudad, @numerodedocumento, @fechahoraingreso, @estado, @sucursalid)";

                        // Parametrización de la consulta
                        cmd.Parameters.Clear();  // Limpiar los parámetros antes de agregar nuevos
                        cmd.Parameters.AddWithValue("codigo", vended.Vended.Trim());
                        cmd.Parameters.AddWithValue("nombre", vended.Nombre.Trim());
                        cmd.Parameters.AddWithValue("telefonos", vended.Tel.Trim());
                        cmd.Parameters.AddWithValue("direccion", vended.Direcc.Trim());
                        cmd.Parameters.AddWithValue("ciudad", vended.Ciudad.Trim());
                        cmd.Parameters.AddWithValue("numerodedocumento", vended.Cedula);
                        cmd.Parameters.AddWithValue("fechahoraingreso", vended.FechIng); // Asegúrate de que la fecha sea del tipo correcto
                        cmd.Parameters.AddWithValue("estado", true); // Por ejemplo, siempre `true` para estado
                        cmd.Parameters.AddWithValue("sucursalid", 10); // Un valor constante para sucursalid (puedes ajustarlo)

                        // Ejecutar el comando de inserción
                        cmd.ExecuteNonQuery();
                    }

                    // Confirmar la transacción
                    tx.Commit();
                }
            }

            Console.WriteLine("Datos insertados correctamente.");
            cronometer.Stop();
            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
        }


        public static async Task InsertarLiqFactItems(List<t_conliqf> conliqf_list, List<t_detliqf> detliqf_list, int batchSize = 2000)
        {
            try
            {
                if (conliqf_list == null || !conliqf_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                if (detliqf_list == null || !detliqf_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    var adetliqf = detliqf_list.GroupBy(x => x.codref).ToDictionary(g => g.Key, g => g.First());

                    // Procesar en lotes
                    while (contador < conliqf_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""liquidacionfacturasitems""(
                                 ""codigo"", 
                                 ""nombre"",
                                 ""estado"",
                                 ""agregarliquidacion"",
                                 ""conceptomanual"",
                                 ""contabilizarconcepto"",
                                 ""naturaleza"",
                                 ""mostrarsaldo"",
                                 ""modificarsaldo"",
                                 ""grupo""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < conliqf_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = conliqf_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @codigo{contador},                                        
                                        @nombre{contador},
                                        @estado{contador},
                                        @agregarliquidacion{contador}, 
                                        @conceptomanual{contador}, 
                                        @contabilizarconcepto{contador}, 
                                        @naturaleza{contador}, 
                                        @mostrarsaldo{contador}, 
                                        @modificarsaldo{contador}, 
                                        @grupo{contador}
                                     )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = conliqf_list[batchStartIndex + i];

                                    if (movi == null) continue;


                                    if (adetliqf.TryGetValue((movi.codref), out var xConLiqf))
                                    {
                                        string xnaturaleza = xConLiqf.naturaleza switch
                                        {
                                            "A" => "D",
                                            "D" => "D",
                                            "B" => "C",
                                            "C" => "C",
                                            _ => DBNull.Value.ToString()
                                        };

                                        cmd.Parameters.AddWithValue($"@naturaleza{batchStartIndex + i}", xnaturaleza);
                                        cmd.Parameters.AddWithValue($"@mostrarsaldo{batchStartIndex + i}", xConLiqf.si_saldo == 1);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para ({movi.codref})");
                                        cmd.Parameters.AddWithValue($"@naturaleza{batchStartIndex + i}", DBNull.Value);
                                        cmd.Parameters.AddWithValue($"@mostrarsaldo{batchStartIndex + i}", false);
                                    }

                                    cmd.Parameters.AddWithValue($"@codigo{batchStartIndex + i}", movi.codref);
                                    cmd.Parameters.AddWithValue($"@nombre{batchStartIndex + i}", movi.nombre);
                                    cmd.Parameters.AddWithValue($"@estado{batchStartIndex + i}", movi.estado == 1);
                                    cmd.Parameters.AddWithValue($"@agregarliquidacion{batchStartIndex + i}", movi.si_agrega == 1);
                                    cmd.Parameters.AddWithValue($"@conceptomanual{batchStartIndex + i}", movi.si_manual == 1);
                                    cmd.Parameters.AddWithValue($"@contabilizarconcepto{batchStartIndex + i}", movi.si_suma == 1);
                                    cmd.Parameters.AddWithValue($"@modificarsaldo{batchStartIndex + i}", movi.modifica == 1);
                                    cmd.Parameters.AddWithValue($"@grupo{batchStartIndex + i}", DBNull.Value);
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{conliqf_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarMLiqFact(int batchSize = 2000)
        {
            try
            {
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""liquidacionfacturasitems""(
                                 ""codigo"", 
                                 ""nombre"",
                                 ""estado"",
                                 ""agregarliquidacion"",
                                 ""conceptomanual"",
                                 ""contabilizarconcepto"",
                                 ""naturaleza"",
                                 ""mostrarsaldo"",
                                 ""modificarsaldo"",
                                 ""grupo""
                                ) VALUES ('A21','Total, factura',TRUE,FALSE,FALSE,FALSE,NULL,FALSE,FALSE,NULL),
                                         ('A41','Total, Faltantes',TRUE,FALSE,FALSE,FALSE,NULL,FALSE,FALSE,NULL),
                                         ('B11','Total, notas crédito averías',TRUE,FALSE,FALSE,FALSE,NULL,FALSE,FALSE,NULL),
                                         ('C11','Total, cartera descontar',TRUE,FALSE,FALSE,FALSE,NULL,FALSE,FALSE,NULL)");

                                cmd.CommandText = commandText.ToString();

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en manual (registros): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarConfigpos(List<t_config5> config5_list, List<t_config> config_list, int xCosPos, int batchSize = 2000)
        {
            try
            {
                if (config5_list == null || !config5_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    var sucursales = new Dictionary<string, int>();     // codigo → id
                    var formaspago = new Dictionary<string, int>();     // Codigo → Id
                    var bodegas = new Dictionary<string, int>();        // Codigo → Id
                    var cajasucursal = new Dictionary<string, int>();   // Codigo → Id
                    var ccosto = new Dictionary<string, int>();         // Codigo → Id

                    // Sucursales
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            sucursales[codigo] = id;
                        }
                    }

                    // formaspago
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM ""FormasPago""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            formaspago[codigo] = id;
                        }
                    }

                    // bodegas
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM bodegas", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            bodegas[codigo] = id;
                        }
                    }

                    // cajasucursal
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""CodigoCaja"" FROM ""CajaSucursal""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            cajasucursal[codigo] = id;
                        }
                    }

                    // Procesar en lotes
                    var coincidencias = config5_list.Where(x => x.cod_pos.Equals(xCosPos.ToString().PadLeft(3, '0'))).ToList();

                    while (contador < coincidencias.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                            INSERT INTO public.""ConfigPOS""(
                                ""SincronizarDB"", 
                                ""PedirBodega"", 
                                ""PedirCantidad"", 
                                ""PedirDescuentoIndividual"", 
                                ""ManejarDescuentoGeneral"", 
                                ""PreguntarPrecio"", 
                                ""PrecioUnitDisplaySuperior"", 
                                ""DecimalesPrecioUnitario"", 
                                ""PermitirPrecioInferior"", 
                                ""PedirFacturaDevolver"", 
                                ""TerminarWindowsAlSalir"", 
                                ""GrabarConEnter"", 
                                ""NoMostrarComandosAyuda"", 
                                ""BarraTareasOculta"", 
                                ""PedirRegistroAgotados"", 
                                ""HabilitarFacturacionEnEspera"", 
                                ""HabilitarPromptImpresionFe"", 
                                ""UtilizarNuevaFuncionCreditos"", 
                                ""UsarFechaControlPos"", 
                                ""ControlFechaLocal"", 
                                ""AyudaProductosEnter"", 
                                ""UsarProductosEnRed"", 
                                ""EditarProductosDesdePos"", 
                                ""ManejarTercero"", 
                                ""TipoPos"", 
                                ""FormaPagoAsociadaTipoPos"", 
                                ""TiempoEsperaImagenEcologica"", 
                                ""TiempoEsperaAvisoFeTercero"", 
                                ""IdBodegaPorDefecto"", 
                                ""IdSucursalPorDefecto"", 
                                ""CodigoPosPorDefecto"", 
                                ""CentroCostoPorDefecto"", 
                                ""DecimalesCantidadDetalle"", 
                                ""ActivarFacElectronicaEnPOS"", 
                                ""NoActivarCupoCreditoQuince"", 
                                ""ActivarCupoGeneralEmpleado"", 
                                ""ImpresoraAdvanced"", 
                                ""LimiteFacturaEnEspera"", 
                                ""ValorBolsa"", 
                                ""tpos"", 
                                ""Activo""
                            ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < coincidencias.Count && currentBatchSize < batchSize)
                                {
                                    var mov = coincidencias[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @sincronizardb{contador}, 
                                        @pedirbodega{contador}, 
                                        @pedircantidad{contador}, 
                                        @pedirdescuentoindividual{contador}, 
                                        @manejardescuentogeneral{contador}, 
                                        @preguntarprecio{contador}, 
                                        @preciounitdisplaysuperior{contador}, 
                                        @decimalespreciounitario{contador}, 
                                        @permitirprecioinferior{contador}, 
                                        @pedirfacturadevolver{contador}, 
                                        @terminarwindowsalsalir{contador}, 
                                        @grabarconenter{contador}, 
                                        @nomostrarcomandosayuda{contador}, 
                                        @barratareasoculta{contador}, 
                                        @pedirregistroagotados{contador}, 
                                        @habilitarfacturacionenespera{contador}, 
                                        @habilitarpromptimpresionfe{contador}, 
                                        @utilizarnuevafuncioncreditos{contador}, 
                                        @usarfechacontrolpos{contador}, 
                                        @controlfechalocal{contador}, 
                                        @ayudaproductosenter{contador}, 
                                        @usarproductosenred{contador}, 
                                        @editarproductosdesdepos{contador}, 
                                        @manejartercero{contador}, 
                                        @tipopos{contador}, 
                                        @formapagoasociadatipopos{contador}, 
                                        @tiempoesperaimagenecologica{contador}, 
                                        @tiempoesperaavisofetercero{contador}, 
                                        @idbodegapordefecto{contador}, 
                                        @idsucursalpordefecto{contador}, 
                                        @codigopospordefecto{contador}, 
                                        @centrocostopordefecto{contador}, 
                                        @decimalescantidaddetalle{contador}, 
                                        @activarfacelectronicaenpos{contador}, 
                                        @noactivarcupocreditoquince{contador}, 
                                        @activarcupogeneralempleado{contador}, 
                                        @impresoraadvanced{contador}, 
                                        @limitefacturaenespera{contador}, 
                                        @valorbolsa{contador}, 
                                        @tpos{contador}, 
                                        @activo{contador}
                                    )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = coincidencias[batchStartIndex + i];
                                    var lvalbol = config_list[1];

                                    if (movi == null) continue;

                                    // sucursales
                                    if (sucursales.TryGetValue((movi.sucursal), out var xsucursal))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdSucursalPorDefecto{batchStartIndex + i}", xsucursal);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para ({movi.sucursal})");
                                        cmd.Parameters.AddWithValue($"@IdSucursalPorDefecto{batchStartIndex + i}", DBNull.Value);
                                    }

                                    //bodegas
                                    if (bodegas.TryGetValue((movi.bodega), out var xbodega))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdBodegaPorDefecto{batchStartIndex + i}", xbodega);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para ({movi.bodega})");
                                        cmd.Parameters.AddWithValue($"@IdBodegaPorDefecto{batchStartIndex + i}", DBNull.Value);
                                    }

                                    //formapagoasociadatipopos
                                    if (bodegas.TryGetValue((movi.fpago_asa), out var xfpago_asa))
                                    {
                                        cmd.Parameters.AddWithValue($"@FormaPagoAsociadaTipoPos{batchStartIndex + i}", xfpago_asa);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para ({movi.bodega})");
                                        cmd.Parameters.AddWithValue($"@FormaPagoAsociadaTipoPos{batchStartIndex + i}", DBNull.Value);
                                    }

                                    //codigopospordefecto
                                    if (bodegas.TryGetValue((movi.cod_pos), out var xcod_pos))
                                    {
                                        cmd.Parameters.AddWithValue($"@CodigoPosPorDefecto{batchStartIndex + i}", xcod_pos);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para ({movi.bodega})");
                                        cmd.Parameters.AddWithValue($"@CodigoPosPorDefecto{batchStartIndex + i}", DBNull.Value);
                                    }

                                    cmd.Parameters.AddWithValue($"@SincronizarDB{batchStartIndex + i}", movi.si_bajar == 1);
                                    cmd.Parameters.AddWithValue($"@PedirBodega{batchStartIndex + i}", movi.si_bodega == 1);
                                    cmd.Parameters.AddWithValue($"@PedirCantidad{batchStartIndex + i}", movi.pide_cant == 1);
                                    cmd.Parameters.AddWithValue($"@PedirDescuentoIndividual{batchStartIndex + i}", movi.pide_desc == 1);
                                    cmd.Parameters.AddWithValue($"@ManejarDescuentoGeneral{batchStartIndex + i}", movi.si_descgen == 1);
                                    cmd.Parameters.AddWithValue($"@PreguntarPrecio{batchStartIndex + i}", movi.preg_prec == 1);
                                    cmd.Parameters.AddWithValue($"@PreciounitDisplaySuperior{batchStartIndex + i}", movi.precio_up == 1);
                                    cmd.Parameters.AddWithValue($"@Decimalespreciounitario{batchStartIndex + i}", movi.deci_puni == 1);
                                    cmd.Parameters.AddWithValue($"@PermitirPrecioInferior{batchStartIndex + i}", false);
                                    cmd.Parameters.AddWithValue($"@PedirFacturaDevolver{batchStartIndex + i}", movi.si_fac_dev == 1);
                                    cmd.Parameters.AddWithValue($"@TerminarWindowsAlSalir{batchStartIndex + i}", movi.win_exit == 1);
                                    cmd.Parameters.AddWithValue($"@GrabarConEnter{batchStartIndex + i}", movi.grab_ent == 1);
                                    cmd.Parameters.AddWithValue($"@NoMostrarComandosAyuda{batchStartIndex + i}", movi.ver_help == 1);
                                    cmd.Parameters.AddWithValue($"@BarraTareasOculta{batchStartIndex + i}", movi.ocultabar == 1);
                                    cmd.Parameters.AddWithValue($"@PediRregistroAgotados{batchStartIndex + i}", movi.ver_agota == 1);
                                    cmd.Parameters.AddWithValue($"@HabilitarFacturacionEnEspera{batchStartIndex + i}", movi.si_esp == 1);
                                    cmd.Parameters.AddWithValue($"@HabilitarPromptImpresionFe{batchStartIndex + i}", movi.si_prompt == 1);
                                    cmd.Parameters.AddWithValue($"@UtilizarNuevaFuncionCreditos{batchStartIndex + i}", movi.nuevocred == 1);
                                    cmd.Parameters.AddWithValue($"@UsarFechaControlPos{batchStartIndex + i}", movi.tipo_fecha == 1);
                                    cmd.Parameters.AddWithValue($"@ControlFechaLocal{batchStartIndex + i}", movi.fec_local == 1);
                                    cmd.Parameters.AddWithValue($"@AyudaProductosEnter{batchStartIndex + i}", movi.ayu_enter == 1);
                                    cmd.Parameters.AddWithValue($"@UsarProductosEnRed{batchStartIndex + i}", movi.items_red == 1);
                                    cmd.Parameters.AddWithValue($"@EditarProductosDesdePos{batchStartIndex + i}", movi.items_add == 1);
                                    cmd.Parameters.AddWithValue($"@ManejarTercero{batchStartIndex + i}", movi.crea_anex);
                                    cmd.Parameters.AddWithValue($"@TipoPos{batchStartIndex + i}", movi.asadero);
                                    //cmd.Parameters.AddWithValue($"@formapagoasociadatipopos{batchStartIndex + i}", movi.fpago_asa);
                                    cmd.Parameters.AddWithValue($"@TiempoEsperaImagenEcologica{batchStartIndex + i}", movi.img_seg);
                                    cmd.Parameters.AddWithValue($"@TiempoEsperaAvisoFeTercero{batchStartIndex + i}", movi.facele_seg);
                                    //cmd.Parameters.AddWithValue($"@codigopospordefecto{batchStartIndex + i}", movi.cod_pos);
                                    cmd.Parameters.AddWithValue($"@CentroCostoPorDefecto{batchStartIndex + i}", movi.ccosto);
                                    cmd.Parameters.AddWithValue($"@DecimalesCantidadDetalle{batchStartIndex + i}", movi.deci_det);
                                    cmd.Parameters.AddWithValue($"@ActivarFacelectronicaEnPos{batchStartIndex + i}", movi.si_facele == 1);
                                    cmd.Parameters.AddWithValue($"@NoActivarCupoCreditoQuince{batchStartIndex + i}", movi.no_act_ccq == 1);
                                    cmd.Parameters.AddWithValue($"@ActivarCupoGeneralEmpleado{batchStartIndex + i}", movi.actqcre == 1);
                                    cmd.Parameters.AddWithValue($"@ImpresoraAdvanced{batchStartIndex + i}", movi.print_adv == 1);
                                    cmd.Parameters.AddWithValue($"@LimiteFacturaEnEspera{batchStartIndex + i}", movi.lim_fesp);
                                    cmd.Parameters.AddWithValue($"@ValorBolsa{batchStartIndex + i}", lvalbol.valimpbo);
                                    cmd.Parameters.AddWithValue($"@tpos{batchStartIndex + i}", movi.asadero);
                                    cmd.Parameters.AddWithValue($"@Activo{batchStartIndex + i}", true);
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{config5_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarConfigposconsecutivos(List<t_config5> config5_list, int xCosPos, int batchSize = 2000)
        {
            try
            {
                if (config5_list == null || !config5_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    var cajasucursal = new Dictionary<string, int>(); // Codigo → Id
                    var Documentos = new Dictionary<string, int>();

                    // cajasucursal
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""CodigoCaja"" FROM ""CajaSucursal""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            cajasucursal[codigo] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Codigo"", ""Id""  FROM public.""Documentos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {

                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);

                            Documentos[codigo] = id;
                        }
                    }


                    // Procesar en lotes
                    var coincidencias = config5_list.Where(x => x.cod_pos.Equals(xCosPos.ToString().PadLeft(3, '0'))).ToList();

                    while (contador < coincidencias.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""ConfigPOSConsecutivos""(
                                    ""IdCaja"", 
                                    ""IdDocTiquetePos"", 
                                    ""TiquetePosUltimo"", 
                                    ""IdDocFacturaElectronica"", 
                                    ""FacturaElectronicaUltimo"", 
                                    ""IdDocFacturaContingencia"", 
                                    ""FacturaContingenciaUltimo"", 
                                    ""IdDocDevolucion"", 
                                    ""DevolucionUltimo"", 
                                    ""IdDocIngresosCaja"", 
                                    ""IngresosCajaUltimo"", 
                                    ""IdDocEgresosCaja"", 
                                    ""EgresosCajaUltimo"", 
                                    ""IdDocReciboCaja"", 
                                    ""ReciboCajaUltimo"", 
                                    ""NoTransaccion""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < coincidencias.Count && currentBatchSize < batchSize)
                                {
                                    var mov = coincidencias[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @idcaja{contador}, 
                                        @iddoctiquetepos{contador}, 
                                        @tiqueteposultimo{contador}, 
                                        @iddocfacturaelectronica{contador}, 
                                        @facturaelectronicaultimo{contador}, 
                                        @iddocfacturacontingencia{contador}, 
                                        @facturacontingenciaultimo{contador}, 
                                        @iddocdevolucion{contador}, 
                                        @devolucionultimo{contador}, 
                                        @iddocingresoscaja{contador}, 
                                        @ingresoscajaultimo{contador}, 
                                        @iddocegresoscaja{contador}, 
                                        @egresoscajaultimo{contador}, 
                                        @iddocrecibocaja{contador}, 
                                        @recibocajaultimo{contador}, 
                                        @notransaccion{contador}
                                    )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = coincidencias[batchStartIndex + i];

                                    if (movi == null) continue;

                                    // IdCaja
                                    if (cajasucursal.TryGetValue((movi.cod_pos), out var xidcaja))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdCaja{batchStartIndex + i}", xidcaja);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró id para ({movi.cod_pos})");
                                        cmd.Parameters.AddWithValue($"@IdCaja{batchStartIndex + i}", DBNull.Value);
                                    }

                                    // IdDocTiquetePos
                                    if (Documentos.TryGetValue((movi.docum_fac), out var xidfac))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdDocTiquetePos{batchStartIndex + i}", xidfac);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró id para ({movi.docum_fac})");
                                        cmd.Parameters.AddWithValue($"@IdDocTiquetePos{batchStartIndex + i}", DBNull.Value);
                                    }

                                    // IdDocFacturaElectronica
                                    if (Documentos.TryGetValue((movi.docum_fac), out var xidfel))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdDocFacturaElectronica{batchStartIndex + i}", xidfel);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró id para ({movi.docum_fel})");
                                        cmd.Parameters.AddWithValue($"@IdDocFacturaElectronica{batchStartIndex + i}", DBNull.Value);
                                    }

                                    // IdDocFacturaContingencia
                                    if (Documentos.TryGetValue((movi.docum_dc), out var xidfdc))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdDocFacturaContingencia{batchStartIndex + i}", xidfdc);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró id para ({movi.docum_dc})");
                                        cmd.Parameters.AddWithValue($"@IdDocFacturaContingencia{batchStartIndex + i}", DBNull.Value);
                                    }

                                    // IdDocDevolucion
                                    if (Documentos.TryGetValue((movi.docum_dev), out var xiddev))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdDocDevolucion{batchStartIndex + i}", xiddev);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró id para ({movi.docum_dev})");
                                        cmd.Parameters.AddWithValue($"@IdDocDevolucion{batchStartIndex + i}", DBNull.Value);
                                    }

                                    // IdDocIngresosCaja
                                    if (Documentos.TryGetValue((movi.docum_otr), out var xiding))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdDocIngresosCaja{batchStartIndex + i}", xiding);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró id para ({movi.docum_otr})");
                                        cmd.Parameters.AddWithValue($"@IdDocIngresosCaja{batchStartIndex + i}", DBNull.Value);
                                    }

                                    // IdDocEgresosCaja
                                    if (Documentos.TryGetValue((movi.docum_egr), out var xidegr))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdDocEgresosCaja{batchStartIndex + i}", xidegr);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró id para ({movi.docum_egr})");
                                        cmd.Parameters.AddWithValue($"@IdDocEgresosCaja{batchStartIndex + i}", DBNull.Value);
                                    }

                                    // IdDocReciboCaja
                                    if (Documentos.TryGetValue((movi.docum_rcj), out var xidrcj))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdDocReciboCaja{batchStartIndex + i}", xidrcj);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró id para ({movi.docum_rcj})");
                                        cmd.Parameters.AddWithValue($"@IdDocReciboCaja{batchStartIndex + i}", DBNull.Value);
                                    }

                                    // Últimos consecutivos
                                    cmd.Parameters.AddWithValue($"@TiquetePosUltimo{batchStartIndex + i}", movi.fact_act);
                                    cmd.Parameters.AddWithValue($"@FacturaElectronicaUltimo{batchStartIndex + i}", movi.ultfact);
                                    cmd.Parameters.AddWithValue($"@FacturaContingenciaUltimo{batchStartIndex + i}", movi.dc_act);
                                    cmd.Parameters.AddWithValue($"@DevolucionUltimo{batchStartIndex + i}", movi.devo_act);
                                    cmd.Parameters.AddWithValue($"@IngresosCajaUltimo{batchStartIndex + i}", movi.otros_act);
                                    cmd.Parameters.AddWithValue($"@EgresosCajaUltimo{batchStartIndex + i}", movi.egres_act);
                                    cmd.Parameters.AddWithValue($"@ReciboCajaUltimo{batchStartIndex + i}", movi.rcj_act);
                                    cmd.Parameters.AddWithValue($"@NoTransaccion{batchStartIndex + i}", movi.transac);
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{config5_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarConfigposotros(List<t_config5> config5_list, int xCosPos, int batchSize = 2000)
        {
            try
            {
                if (config5_list == null || !config5_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    var formasdepago = new Dictionary<string, int>(); // Codigo → Id

                    // formasdepago
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM ""FormasPago""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            formasdepago[codigo] = id;
                        }
                    }

                    // Procesar en lotes
                    var coincidencias = config5_list.Where(x => x.cod_pos.Equals(xCosPos.ToString().PadLeft(3, '0'))).ToList();

                    while (contador < coincidencias.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""ConfigPOSOtros""(
                                    ""BajarDatosDisco"", 
                                    ""BajarDatosDiscoCantidad"", 
                                    ""IdFormaPago1"", 
                                    ""IdFormaPago2"", 
                                    ""PuertoCOM"", 
                                    ""MensajeFactura1"", 
                                    ""MensajeFactura2"", 
                                    ""AperturaMonedero"", 
                                    ""CortePapel"", 
                                    ""EspaciosAntesTitulo"", 
                                    ""RenglonesFinalizarImpresion"", 
                                    ""FechaApertura"", 
                                    ""ClaveApertura"", 
                                    ""ModoApertura"", 
                                    ""IntentosApertura"", 
                                    ""EstadoCaja""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < coincidencias.Count && currentBatchSize < batchSize)
                                {
                                    var mov = coincidencias[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"( 
                                        @bajardatosdisco{contador}, 
                                        @bajardatosdiscocantidad{contador}, 
                                        @idformapago1{contador}, 
                                        @idformapago2{contador}, 
                                        @puertocom{contador}, 
                                        @mensajefactura1{contador}, 
                                        @mensajefactura2{contador}, 
                                        @aperturamonedero{contador}, 
                                        @cortepapel{contador}, 
                                        @espaciosantestitulo{contador}, 
                                        @renglonesfinalizarimpresion{contador}, 
                                        @fechaapertura{contador}, 
                                        @claveapertura{contador}, 
                                        @modoapertura{contador}, 
                                        @intentosapertura{contador}, 
                                        @estadocaja{contador}
                                    )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = coincidencias[batchStartIndex + i];

                                    if (movi == null) continue;

                                    // formasdepago
                                    if (formasdepago.TryGetValue((movi.fp_pred1), out var xidfp1))
                                    {
                                        cmd.Parameters.AddWithValue($"@idformapago1{batchStartIndex + i}", xidfp1);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró id para ({movi.fp_pred1})");
                                        cmd.Parameters.AddWithValue($"@idcaja{batchStartIndex + i}", DBNull.Value);
                                    }

                                    // formasdepago
                                    if (formasdepago.TryGetValue((movi.fp_pred2), out var xidfp2))
                                    {
                                        cmd.Parameters.AddWithValue($"@idformapago2{batchStartIndex + i}", xidfp2);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró id para ({movi.fp_pred2})");
                                        cmd.Parameters.AddWithValue($"@idcaja{batchStartIndex + i}", DBNull.Value);
                                    }

                                    cmd.Parameters.AddWithValue($"@bajardatosdisco{batchStartIndex + i}", movi.cerr_grab);
                                    cmd.Parameters.AddWithValue($"@bajardatosdiscocantidad{batchStartIndex + i}", movi.interv_cie);
                                    cmd.Parameters.AddWithValue($"@puertocom{batchStartIndex + i}", movi.portcalif);
                                    cmd.Parameters.AddWithValue($"@mensajefactura1{batchStartIndex + i}", movi.mens1);
                                    cmd.Parameters.AddWithValue($"@mensajefactura2{batchStartIndex + i}", movi.mens2);
                                    cmd.Parameters.AddWithValue($"@aperturamonedero{batchStartIndex + i}", movi.cod_cash);
                                    cmd.Parameters.AddWithValue($"@cortepapel{batchStartIndex + i}", movi.cod_papel);
                                    cmd.Parameters.AddWithValue($"@espaciosantestitulo{batchStartIndex + i}", movi.espac_tit1);
                                    cmd.Parameters.AddWithValue($"@renglonesfinalizarimpresion{batchStartIndex + i}", movi.n_lineblan);
                                    cmd.Parameters.AddWithValue($"@fechaapertura{batchStartIndex + i}", movi.fechaaper);
                                    cmd.Parameters.AddWithValue($"@claveapertura{batchStartIndex + i}", movi.claveaper);
                                    cmd.Parameters.AddWithValue($"@modoapertura{batchStartIndex + i}", movi.modoaper);
                                    cmd.Parameters.AddWithValue($"@intentosapertura{batchStartIndex + i}", movi.lmte_aper);
                                    cmd.Parameters.AddWithValue($"@estadocaja{batchStartIndex + i}", 1);
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{config5_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarConfigposteff(List<t_config5> config5_list, int xCosPos, int batchSize = 2000)
        {
            try
            {
                if (config5_list == null || !config5_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Procesar en lotes
                    var coincidencias = config5_list.Where(x => x.cod_pos.Equals(xCosPos.ToString().PadLeft(3, '0'))).ToList();

                    while (contador < coincidencias.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""ConfigPOSTeff""(
                                    ""ModuloTeffExistente"", 
                                    ""PuertoCOM"", 
                                    ""Velocidad"", 
                                    ""Paridad"", 
                                    ""BitsDatos"", 
                                    ""BitsParada"", 
                                    ""TiempoTimeOut"", 
                                    ""TiempoSleep"", 
                                    ""ModoOperacion"", 
                                    ""NumeroDatafono"", 
                                    ""ModoAutomatico"", 
                                    ""ModoManual"", 
                                    ""EnviarCajeroEnProceso"", 
                                    ""RutaCarpetaEjecutable"", 
                                    ""NombreArchivoEjecutable"", 
                                    ""NombreArchivoSolicitud"", 
                                    ""NombreArchivoRespuesta"", 
                                    ""IPTerminal"", 
                                    ""NumeroTerminalBancaria"", 
                                    ""NumeroTerminalCorresponsal""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < coincidencias.Count && currentBatchSize < batchSize)
                                {
                                    var mov = coincidencias[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @moduloteffexistente{contador}, 
                                        @puertocom{contador}, 
                                        @velocidad{contador}, 
                                        @paridad{contador}, 
                                        @bitsdatos{contador}, 
                                        @bitsparada{contador}, 
                                        @tiempotimeout{contador}, 
                                        @tiemposleep{contador}, 
                                        @modooperacion{contador}, 
                                        @numerodatafono{contador}, 
                                        @modoautomatico{contador}, 
                                        @modomanual{contador}, 
                                        @enviarcajeroenproceso{contador}, 
                                        @rutacarpetaejecutable{contador}, 
                                        @nombrearchivoejecutable{contador}, 
                                        @nombrearchivosolicitud{contador}, 
                                        @nombrearchivorespuesta{contador}, 
                                        @ipterminal{contador}, 
                                        @numeroterminalbancaria{contador}, 
                                        @numeroterminalcorresponsal{contador}
                                    )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = coincidencias[batchStartIndex + i];

                                    if (movi == null) continue;

                                    cmd.Parameters.AddWithValue($"@moduloteffexistente{batchStartIndex + i}", movi.tef_activo == 1);
                                    cmd.Parameters.AddWithValue($"@puertocom{batchStartIndex + i}", int.Parse(movi.tef_port));
                                    cmd.Parameters.AddWithValue($"@velocidad{batchStartIndex + i}", movi.tef_veloc);
                                    cmd.Parameters.AddWithValue($"@paridad{batchStartIndex + i}", movi.tef_parid);
                                    cmd.Parameters.AddWithValue($"@bitsdatos{batchStartIndex + i}", int.Parse(movi.tef_bitdat));
                                    cmd.Parameters.AddWithValue($"@bitsparada{batchStartIndex + i}", int.Parse(movi.tef_stop));
                                    cmd.Parameters.AddWithValue($"@tiempotimeout{batchStartIndex + i}", movi.tef_time);
                                    cmd.Parameters.AddWithValue($"@tiemposleep{batchStartIndex + i}", DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@modooperacion{batchStartIndex + i}", movi.tef_silen);
                                    cmd.Parameters.AddWithValue($"@numerodatafono{batchStartIndex + i}", movi.datafono);
                                    cmd.Parameters.AddWithValue($"@modoautomatico{batchStartIndex + i}", movi.mod_teff == 1);
                                    cmd.Parameters.AddWithValue($"@modomanual{batchStartIndex + i}", movi.mod_teff == 2);
                                    cmd.Parameters.AddWithValue($"@enviarcajeroenproceso{batchStartIndex + i}", movi.tef_sicaj == 1);
                                    cmd.Parameters.AddWithValue($"@rutacarpetaejecutable{batchStartIndex + i}", movi.tef_dir);
                                    cmd.Parameters.AddWithValue($"@nombrearchivoejecutable{batchStartIndex + i}", movi.tef_exe);
                                    cmd.Parameters.AddWithValue($"@nombrearchivosolicitud{batchStartIndex + i}", movi.tef_solic);
                                    cmd.Parameters.AddWithValue($"@nombrearchivorespuesta{batchStartIndex + i}", movi.tef_respu);
                                    cmd.Parameters.AddWithValue($"@ipterminal{batchStartIndex + i}", DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@numeroterminalbancaria{batchStartIndex + i}", movi.dat_banco);
                                    cmd.Parameters.AddWithValue($"@numeroterminalcorresponsal{batchStartIndex + i}", movi.dat_cnb);
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{coincidencias.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarConfigbalanza(List<t_config5> config5_list, int xCosPos, int batchSize = 2000)
        {
            try
            {
                if (config5_list == null || !config5_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Procesar en lotes
                    var coincidencias = config5_list.Where(x => x.cod_pos.Equals(xCosPos.ToString().PadLeft(3, '0'))).ToList();

                    while (contador < coincidencias.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""ConfigBalanza""(
                                    ""PuertoCOMBalanza"", 
                                    ""SingleCable"", 
                                    ""UtilizaDDE"", 
                                    ""Server"", 
                                    ""Servicio"", 
                                    ""Categoria"", 
                                    ""Comando"", 
                                    ""ModoNativo"", 
                                    ""CodigoControl"", 
                                    ""UsaCamaraVigilancia"", 
                                    ""PuertoCOM"", 
                                    ""UsaBalanzaUrano"", 
                                    ""ModeloBalanza"", 
                                    ""TipoBalanza"", 
                                    ""VersionScanner"", 
                                    ""VersionScale"", 
                                    ""Timeout"", 
                                    ""PuertoScanner"", 
                                    ""PuertoScale"", 
                                    ""ConfigScale"", 
                                    ""ConfigScanner""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < coincidencias.Count && currentBatchSize < batchSize)
                                {
                                    var mov = coincidencias[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(  
                                        @puertocombalanza{contador}, 
                                        @singlecable{contador}, 
                                        @utilizadde{contador}, 
                                        @server{contador}, 
                                        @servicio{contador}, 
                                        @categoria{contador}, 
                                        @comando{contador}, 
                                        @modonativo{contador}, 
                                        @codigocontrol{contador}, 
                                        @usacamaravigilancia{contador}, 
                                        @puertocom{contador}, 
                                        @usabalanzaurano{contador}, 
                                        @modelobalanza{contador}, 
                                        @tipobalanza{contador}, 
                                        @versionscanner{contador}, 
                                        @versionscale{contador}, 
                                        @timeout{contador}, 
                                        @puertoscanner{contador}, 
                                        @puertoscale{contador}, 
                                        @configscale{contador}, 
                                        @configscanner{contador}
                                    )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = coincidencias[batchStartIndex + i];

                                    if (movi == null) continue;

                                    cmd.Parameters.AddWithValue($"@puertocombalanza{batchStartIndex + i}", movi.scale_com);
                                    cmd.Parameters.AddWithValue($"@singlecable{batchStartIndex + i}", movi.tipo_scale == 1);
                                    cmd.Parameters.AddWithValue($"@utilizadde{batchStartIndex + i}", movi.si_dde == 1);
                                    cmd.Parameters.AddWithValue($"@server{batchStartIndex + i}", movi.exe_dde);
                                    cmd.Parameters.AddWithValue($"@servicio{batchStartIndex + i}", movi.serv_dde);
                                    cmd.Parameters.AddWithValue($"@categoria{batchStartIndex + i}", movi.categdde);
                                    cmd.Parameters.AddWithValue($"@comando{batchStartIndex + i}", movi.comandde);
                                    cmd.Parameters.AddWithValue($"@modonativo{batchStartIndex + i}", movi.scale_set);
                                    cmd.Parameters.AddWithValue($"@codigocontrol{batchStartIndex + i}", movi.scale_code);
                                    cmd.Parameters.AddWithValue($"@usacamaravigilancia{batchStartIndex + i}", movi.si_dde_2 == 1);
                                    cmd.Parameters.AddWithValue($"@puertocom{batchStartIndex + i}", movi.camara_com);
                                    cmd.Parameters.AddWithValue($"@usabalanzaurano{batchStartIndex + i}", movi.usaurano == 1);
                                    cmd.Parameters.AddWithValue($"@modelobalanza{batchStartIndex + i}", movi.tipobal);
                                    cmd.Parameters.AddWithValue($"@tipobalanza{batchStartIndex + i}", movi.ms2430);
                                    cmd.Parameters.AddWithValue($"@versionscanner{batchStartIndex + i}", movi.vscanner);
                                    cmd.Parameters.AddWithValue($"@versionscale{batchStartIndex + i}", movi.vscale);
                                    cmd.Parameters.AddWithValue($"@timeout{batchStartIndex + i}", movi.time_2430);
                                    cmd.Parameters.AddWithValue($"@puertoscanner{batchStartIndex + i}", movi.pscanner);
                                    cmd.Parameters.AddWithValue($"@puertoscale{batchStartIndex + i}", movi.pscale);
                                    cmd.Parameters.AddWithValue($"@configscale{batchStartIndex + i}", "{\"Puerto\":\"COM3\",\"BaudRate\":9600,\"DataBits\":7,\"Parity\":2,\"StopBits\":0,\"Handshake\":0,\"WriteTimeout\":1024,\"ReadTimeout\":512,\"TipoPuerto\":null}");
                                    cmd.Parameters.AddWithValue($"@configscanner{batchStartIndex + i}", "{\"Puerto\":\"COM4\",\"BaudRate\":9600,\"DataBits\":8,\"Parity\":0,\"StopBits\":0,\"Handshake\":1,\"WriteTimeout\":1024,\"ReadTimeout\":512,\"TipoPuerto\":null}");
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{config5_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarLocalizacion(List<t_localiza> localizacion_list, int batchSize = 2000)
        {
            try
            {
                if (localizacion_list == null || !localizacion_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Procesar en lotes
                    while (contador < localizacion_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""localizacion""(
                                    ""descripcion""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < localizacion_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = localizacion_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @descripcion{contador}
                                    )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = localizacion_list[batchStartIndex + i];

                                    if (movi == null) continue;

                                    cmd.Parameters.AddWithValue($"@descripcion{batchStartIndex + i}", movi.nom_loc);
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{localizacion_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarUbicacion(List<t_ubicacion> ubicacion_list, int psucursal, int batchSize = 2000)
        {
            try
            {
                if (ubicacion_list == null || !ubicacion_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    var sucursales = new Dictionary<string, int>();     // codigo → id

                    // Sucursales
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            sucursales[codigo] = id;
                        }
                    }

                    // Procesar en lotes
                    while (contador < ubicacion_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""ubicacion""(
                                    ""descripcion"", 
                                    ""IdSucursal"", 
                                    ""Codigo""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < ubicacion_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = ubicacion_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @descripcion{contador}, 
                                        @IdSucursal{contador}, 
                                        @Codigo{contador}
                                    )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = ubicacion_list[batchStartIndex + i];

                                    if (movi == null) continue;

                                    // sucursales
                                    if (sucursales.TryGetValue((psucursal.ToString().PadLeft(2, '0')), out var xsucursal))
                                    {
                                        cmd.Parameters.AddWithValue($"@idsucursal{batchStartIndex + i}", xsucursal);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para ({psucursal.ToString().PadLeft(2, '0')})");
                                        cmd.Parameters.AddWithValue($"@idsucursal{batchStartIndex + i}", DBNull.Value);
                                    }

                                    cmd.Parameters.AddWithValue($"@descripcion{batchStartIndex + i}", movi.nom_ubi);
                                    cmd.Parameters.AddWithValue($"@codigo{batchStartIndex + i}", movi.cod_ubi.ToString().PadLeft(2, '0'));
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{ubicacion_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarIsis(List<t_cabcaptu> cabcaptu_list, int batchSize = 2000)
        {
            try
            {
                if (cabcaptu_list == null || !cabcaptu_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    var sucursales = new Dictionary<string, int>();     // codigo → id

                    // Sucursales
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            sucursales[codigo] = id;
                        }
                    }

                    // Procesar en lotes
                    while (contador < cabcaptu_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""Isis""(
                                    ""IdSucursal"", 
                                    ""FechaConteo"", 
                                    ""Estado"", 
                                    ""Observacion""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < cabcaptu_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = cabcaptu_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @idsucursal{contador}, 
                                        @fechaconteo{contador}, 
                                        @estado{contador}, 
                                        @observacion{contador}
                                    )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = cabcaptu_list[batchStartIndex + i];

                                    if (movi == null) continue;

                                    // sucursales
                                    if (sucursales.TryGetValue((movi.sucursal), out var xsucursal))
                                    {
                                        cmd.Parameters.AddWithValue($"@idsucursal{batchStartIndex + i}", xsucursal);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para ({movi.sucursal})");
                                        cmd.Parameters.AddWithValue($"@idsucursal{batchStartIndex + i}", DBNull.Value);
                                    }

                                    cmd.Parameters.AddWithValue($"@fechaconteo{batchStartIndex + i}", movi.fecha);
                                    cmd.Parameters.AddWithValue($"@estado{batchStartIndex + i}", movi.estado == 1);
                                    cmd.Parameters.AddWithValue($"@observacion{batchStartIndex + i}", movi.observac);
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{cabcaptu_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarConteoIsis(List<t_detcaptu> detcaptu_list, List<t_cabcaptu> cabcaptu_list, List<t_localiza> localizacion_list, List<t_ubicacion> ubicacion_list, int batchSize = 2000)
        {
            try
            {
                if (detcaptu_list == null || !detcaptu_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Sucursales
                    var sucursales = new Dictionary<string, int>();     // codigo → id

                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            sucursales[codigo] = id;
                        }
                    }

                    // isis
                    var isis = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""IdSucursal"", ""FechaConteo"", ""Observacion"" FROM ""Isis""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);

                            string sucursal = reader.GetInt32(1).ToString();
                            DateTime fechaDt = reader.GetDateTime(2);
                            string fecha = fechaDt.ToString("yyyy-MM-dd HH:mm");
                            string observacion = reader.GetString(3).Trim();

                            string clave = $"{sucursal}|{fecha}|{observacion}";
                            isis[clave] = id;
                        }
                    }

                    // localizacion
                    var localizacion = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT id, descripcion FROM ""localizacion""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string descripcion = reader.GetString(1);
                            localizacion[descripcion] = id;
                        }
                    }

                    // ubicacion
                    var ubicacion = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT id, descripcion, ""IdSucursal"" FROM ""ubicacion""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string descripcion = reader.GetString(1);
                            string sucursal = reader.GetInt32(2).ToString();
                            string clave = $"{sucursal}|{descripcion}";
                            ubicacion[clave] = id;
                        }
                    }

                    // Procesar en lotes
                    while (contador < detcaptu_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""ConteoIsis""(
                                     ""IdUbicacion"", 
                                     ""IdLocalizacion"", 
                                     ""IdNoLocalizacion"", 
                                     ""Observacion"", 
                                     ""ConteoId""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < detcaptu_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = detcaptu_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @idubicacion{contador}, 
                                        @idlocalizacion{contador}, 
                                        @idnolocalizacion{contador}, 
                                        @observacion{contador}, 
                                        @conteoid{contador}
                                    )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = detcaptu_list[batchStartIndex + i];

                                    if (movi == null) continue;

                                    // conteoid
                                    var isis_list = cabcaptu_list.FirstOrDefault(x => x.cod_unico == movi.cod_unico);

                                    if (!sucursales.TryGetValue(isis_list.sucursal, out var xIdsucursal))
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({xIdsucursal})");
                                    }

                                    string fechaFormateada = isis_list.fecha.ToString("yyyy-MM-dd HH:mm");
                                    string claveisis = $"{xIdsucursal}|{fechaFormateada}|{isis_list.observac}";

                                    if (isis.TryGetValue(claveisis, out var xIdisis))
                                    {
                                        cmd.Parameters.AddWithValue($"@conteoid{batchStartIndex + i}", xIdisis);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({claveisis})");
                                        cmd.Parameters.AddWithValue($"@conteoid{batchStartIndex + i}", DBNull.Value);
                                    }

                                    //idubicacion
                                    var ubica_list = ubicacion_list.FirstOrDefault(x => x.cod_ubi == movi.cod_ubi);
                                    string claveubicacion = $"{xIdsucursal}|{ubica_list.nom_ubi}";

                                    if (ubicacion.TryGetValue(claveubicacion, out var xIdubi))
                                    {
                                        cmd.Parameters.AddWithValue($"@idubicacion{batchStartIndex + i}", xIdubi);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({claveubicacion})");
                                        cmd.Parameters.AddWithValue($"@idubicacion{batchStartIndex + i}", DBNull.Value);
                                    }

                                    //idlocalizacion
                                    var localiza_list = localizacion_list.FirstOrDefault(x => x.cod_loc == movi.cod_loc);

                                    if (localizacion.TryGetValue(localiza_list.nom_loc, out var xIdloc))
                                    {
                                        cmd.Parameters.AddWithValue($"@idlocalizacion{batchStartIndex + i}", xIdloc);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({xIdloc})");
                                        cmd.Parameters.AddWithValue($"@idlocalizacion{batchStartIndex + i}", DBNull.Value);
                                    }

                                    cmd.Parameters.AddWithValue($"@idnolocalizacion{batchStartIndex + i}", int.Parse(movi.nom_det.Trim().Where(char.IsDigit).ToArray()));
                                    cmd.Parameters.AddWithValue($"@observacion{batchStartIndex + i}", movi.observac);
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{detcaptu_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarUsuarioIsis(List<t_usucaptu> usucaptu_list, List<t_detcaptu> detcaptu_list, List<t_cabcaptu> cabcaptu_list, List<t_localiza> localizacion_list, int batchSize = 2000)
        {
            try
            {
                if (usucaptu_list == null || !usucaptu_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Sucursales
                    var sucursales = new Dictionary<string, int>();     // codigo → id

                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            sucursales[codigo] = id;
                        }
                    }

                    // isis
                    var isis = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""IdSucursal"", ""FechaConteo"", ""Observacion"" FROM ""Isis""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);

                            string sucursal = reader.GetInt32(1).ToString();
                            DateTime fechaDt = reader.GetDateTime(2);
                            string fecha = fechaDt.ToString("yyyy-MM-dd HH:mm");
                            string observacion = reader.GetString(3).Trim();

                            string clave = $"{sucursal}|{fecha}|{observacion}";
                            isis[clave] = id;
                        }
                    }

                    // localizacion
                    var localizacion = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT id, descripcion FROM ""localizacion""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string descripcion = reader.GetString(1);
                            localizacion[descripcion] = id;
                        }
                    }

                    // Usuarios
                    var usuario = new Dictionary<string, string>();

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""UserName"" FROM ""AspNetUsers""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string id = reader.GetString(0);
                            string descripcion = reader.GetString(1);
                            usuario[descripcion] = id;
                        }
                    }

                    // Procesar en lotes
                    while (contador < usucaptu_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""UsuarioxConteo""(
                                    ""IdConteo"", 
                                    ""ProgramacionConteo"", 
                                    ""IdIsis"", 
                                    ""IdUsuario"", 
                                    ""IdLocalizacion""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < usucaptu_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = usucaptu_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @idconteo{contador}, 
                                        @programacionconteo{contador}, 
                                        @idisis{contador}, 
                                        @idusuario{contador}, 
                                        @idlocalizacion{contador}
                                    )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = usucaptu_list[batchStartIndex + i];

                                    if (movi == null) continue;

                                    var conteoisis_list = detcaptu_list.FirstOrDefault(x => x.cod_unico == movi.cod_unico);

                                    // idisis
                                    var isis_list = cabcaptu_list.FirstOrDefault(x => x.cod_unico == movi.cod_unico);

                                    if (!sucursales.TryGetValue(isis_list.sucursal, out var xIdsucursal))
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({xIdsucursal})");
                                    }

                                    string fechaFormateada = isis_list.fecha.ToString("yyyy-MM-dd HH:mm");
                                    string claveisis = $"{xIdsucursal}|{fechaFormateada}|{isis_list.observac}";

                                    if (isis.TryGetValue(claveisis, out var xIdisis))
                                    {
                                        cmd.Parameters.AddWithValue($"@idisis{batchStartIndex + i}", xIdisis);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({claveisis})");
                                        cmd.Parameters.AddWithValue($"@idisis{batchStartIndex + i}", DBNull.Value);
                                    }

                                    //idlocalizacion
                                    var localiza_list = localizacion_list.FirstOrDefault(x => x.cod_loc == conteoisis_list.cod_loc);
                                    if (localizacion.TryGetValue(localiza_list.nom_loc, out var xIdloc))
                                    {
                                        cmd.Parameters.AddWithValue($"@idlocalizacion{batchStartIndex + i}", xIdloc);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({localiza_list.nom_loc})");
                                        cmd.Parameters.AddWithValue($"@idlocalizacion{batchStartIndex + i}", DBNull.Value);
                                    }

                                    //idusuario
                                    if (usuario.TryGetValue(movi.usuario, out var xIduser))
                                    {
                                        cmd.Parameters.AddWithValue($"@idusuario{batchStartIndex + i}", xIduser);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({movi.usuario})");
                                        cmd.Parameters.AddWithValue($"@idusuario{batchStartIndex + i}", DBNull.Value);
                                    }

                                    cmd.Parameters.AddWithValue($"@idconteo{batchStartIndex + i}", movi.conteo);
                                    cmd.Parameters.AddWithValue($"@programacionconteo{batchStartIndex + i}", 0);

                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{usucaptu_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarControlInventarioIsis(List<t_isiscaptu> isiscaptu_list, List<t_detcaptu> detcaptu_list, List<t_cabcaptu> cabcaptu_list, List<t_localiza> localizacion_list, List<t_ubicacion> ubicacion_list, int batchSize = 2000)
        {
            try
            {
                if (isiscaptu_list == null || !isiscaptu_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Sucursales
                    var sucursales = new Dictionary<string, int>();     // codigo → id

                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            sucursales[codigo] = id;
                        }
                    }

                    // isis
                    var isis = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""IdSucursal"", ""FechaConteo"", ""Observacion"" FROM ""Isis""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);

                            string sucursal = reader.GetInt32(1).ToString();
                            DateTime fechaDt = reader.GetDateTime(2);
                            string fecha = fechaDt.ToString("yyyy-MM-dd HH:mm");
                            string observacion = reader.GetString(3).Trim();

                            string clave = $"{sucursal}|{fecha}|{observacion}";
                            isis[clave] = id;
                        }
                    }

                    // conteoisis
                    var conteoisis = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""IdUbicacion"", ""IdLocalizacion"", ""IdNoLocalizacion"", ""ConteoId"" FROM ""ConteoIsis""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);

                            string idubicacion = reader.GetInt32(1).ToString();
                            string idlocalizacion = reader.GetInt32(2).ToString();
                            string idnolocalizacion = reader.GetInt32(3).ToString();
                            string conteoid = reader.GetInt32(4).ToString();

                            string clave = $"{idubicacion}|{idlocalizacion}|{idnolocalizacion}|{conteoid}";
                            isis[clave] = id;
                        }
                    }

                    // localizacion
                    var localizacion = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT id, descripcion FROM ""localizacion""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string descripcion = reader.GetString(1);
                            localizacion[descripcion] = id;
                        }
                    }

                    // Usuarios
                    var usuario = new Dictionary<string, string>();

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""UserName"" FROM ""AspNetUsers""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string id = reader.GetString(0);
                            string descripcion = reader.GetString(1);
                            usuario[descripcion] = id;
                        }
                    }

                    // ubicacion
                    var ubicacion = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT id, descripcion, ""IdSucursal"" FROM ""ubicacion""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string descripcion = reader.GetString(1);
                            string sucursal = reader.GetInt32(2).ToString();
                            string clave = $"{sucursal}|{descripcion}";
                            ubicacion[clave] = id;
                        }
                    }

                    var productos = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM ""Productos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            productos[codigo] = id;
                        }
                    }

                    // Procesar en lotes
                    while (contador < isiscaptu_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""ControlInventarioIsis""(
                                    ""Fecha"",
                                    ""Codigo"", 
                                    ""UnidadesSueltas"", 
                                    ""Cajas"", 
                                    ""Embalaje"", 
                                    ""Cantidad"", 
                                    ""IdProducto"", 
                                    ""IdConteoIsis"", 
                                    ""IdUsuario"", 
                                    ""ConteoActual""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < isiscaptu_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = isiscaptu_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @fecha{contador}, 
                                        @codigo{contador}, 
                                        @unidadessueltas{contador}, 
                                        @cajas{contador}, 
                                        @embalaje{contador}, 
                                        @cantidad{contador}, 
                                        @idproducto{contador}, 
                                        @idconteoisis{contador}, 
                                        @idusuario{contador}, 
                                        @conteoactual{contador}
                                    )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = isiscaptu_list[batchStartIndex + i];

                                    if (movi == null) continue;

                                    var conteoisis_list = detcaptu_list.FirstOrDefault(x => x.cod_unico == movi.cod_unico);

                                    // idisis
                                    var isis_list = cabcaptu_list.FirstOrDefault(x => x.cod_unico == movi.cod_unico);

                                    if (!sucursales.TryGetValue(isis_list.sucursal, out var xIdsucursal))
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({xIdsucursal})");
                                    }

                                    string fechaFormateada = isis_list.fecha.ToString("yyyy-MM-dd HH:mm");
                                    string claveisis = $"{xIdsucursal}|{fechaFormateada}|{isis_list.observac}";

                                    if (!isis.TryGetValue(claveisis, out var xIdisis))
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({claveisis})");
                                    }

                                    //idlocalizacion
                                    var localiza_list = localizacion_list.FirstOrDefault(x => x.cod_loc == conteoisis_list.cod_loc);
                                    if (!localizacion.TryGetValue(localiza_list.nom_loc, out var xIdloc))
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({localiza_list.nom_loc})");
                                    }

                                    //idubicacion
                                    var ubica_list = ubicacion_list.FirstOrDefault(x => x.cod_ubi == conteoisis_list.cod_ubi);
                                    string claveubicacion = $"{xIdsucursal}|{ubica_list.nom_ubi}";

                                    if (!ubicacion.TryGetValue(claveubicacion, out var xIdubi))
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({claveubicacion})");
                                    }

                                    // idconteoisis
                                    int xidnolocaliza = int.Parse(conteoisis_list.nom_det.Trim().Where(char.IsDigit).ToArray());
                                    string claveconteo = $"{xIdubi}|{xIdloc}|{xidnolocaliza}|{xIdisis}";

                                    if (isis.TryGetValue(claveconteo, out var xIdconteo))
                                    {
                                        cmd.Parameters.AddWithValue($"@idconteoisis{batchStartIndex + i}", xIdconteo);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({claveisis})");
                                        cmd.Parameters.AddWithValue($"@idconteoisis{batchStartIndex + i}", DBNull.Value);
                                    }

                                    //idproducto
                                    if (productos.TryGetValue(movi.codigo, out var xIdcodigo))
                                    {
                                        cmd.Parameters.AddWithValue($"@idproducto{batchStartIndex + i}", xIdcodigo);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({movi.usuario})");
                                        cmd.Parameters.AddWithValue($"@idproducto{batchStartIndex + i}", DBNull.Value);
                                    }

                                    //idusuario
                                    if (usuario.TryGetValue(movi.usuario, out var xIduser))
                                    {
                                        cmd.Parameters.AddWithValue($"@idusuario{batchStartIndex + i}", xIduser);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({movi.usuario})");
                                        cmd.Parameters.AddWithValue($"@idusuario{batchStartIndex + i}", DBNull.Value);
                                    }

                                    cmd.Parameters.AddWithValue($"@fecha{batchStartIndex + i}", movi.fecha);
                                    cmd.Parameters.AddWithValue($"@codigo{batchStartIndex + i}", movi.codigo);
                                    cmd.Parameters.AddWithValue($"@unidadessueltas{batchStartIndex + i}", 0);
                                    cmd.Parameters.AddWithValue($"@cajas{batchStartIndex + i}", 0);
                                    cmd.Parameters.AddWithValue($"@embalaje{batchStartIndex + i}", 0);
                                    cmd.Parameters.AddWithValue($"@cantidad{batchStartIndex + i}", movi.cantidad);
                                    cmd.Parameters.AddWithValue($"@conteoactual{batchStartIndex + i}", movi.conteo);
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{isiscaptu_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static void insertarSeparadosmercanciaabonos(List<t_separados> separados_list, int batchSize = 2000)
        {
            try
            {
                if (separados_list == null || !separados_list.Any())
                {
                    Console.WriteLine("La lista de configuracion está vacía o es null");
                    return;
                }

                int contador = 0;
                int registrosInsertados = 0;
                int registrosOmitidos = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
                {
                    conn.Open();

                    while (contador < separados_list.Count)
                    {
                        using (var tx = conn.BeginTransaction())
                        using (var cmd = new NpgsqlCommand())
                        {
                            cmd.Connection = conn;
                            cmd.Transaction = tx;

                            var commandText = new StringBuilder();
                            commandText.AppendLine(@"
                                INSERT INTO separadomercanciaabonos (
                                    cajasmovimientosid,
                                    separadomercanciaid
                                )VALUES");

                            var valuesList = new List<string>();
                            var registrosValidosEnLote = new List<t_separados>();
                            int currentBatchSize = 0;

                            while (contador < separados_list.Count && currentBatchSize < batchSize)
                            {
                                var separados = separados_list[contador];

                                // Verificar si existe el registro en cajasmovimientos
                                using (var checkCmd = new NpgsqlCommand(
                                "SELECT COUNT(*) FROM public.cajasmovimientos WHERE numerofactura = @cajasmovimientosid AND documentofactura = @documentofactura",
                                conn, tx))
                                {
                                    checkCmd.Parameters.AddWithValue("@cajasmovimientosid", separados.cajasmovimientosid.ToString());
                                    checkCmd.Parameters.AddWithValue("@documentofactura", separados.documentofactura);
                                    var existe = Convert.ToInt32(checkCmd.ExecuteScalar()) > 0;

                                    if (existe)
                                    {
                                        registrosValidosEnLote.Add(separados);

                                        var values = $@"
                                        (
                                            (SELECT id FROM public.cajasmovimientos 
                                                WHERE numerofactura = @cajasmovimientosid{registrosValidosEnLote.Count - 1} 
                                                  AND documentofactura = @documentofactura{registrosValidosEnLote.Count - 1}
                                                LIMIT 1),

                                            (SELECT s.id
                                                FROM public.separadomercancia s
                                                INNER JOIN public.cajasmovimientos c ON s.cajasmovimientosid = c.id
                                                WHERE c.numerofactura = @cajasmovimientosid{registrosValidosEnLote.Count - 1}
                                                  AND c.documentofactura = @documentofactura{registrosValidosEnLote.Count - 1}
                                                LIMIT 1)
                                        )";

                                        valuesList.Add(values);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠ Omitiendo registro: Número {separados.cajasmovimientosid} no existe en cajasmovimientos");
                                        registrosOmitidos++;
                                    }
                                }

                                contador++;
                                currentBatchSize++;
                            }

                            // Solo ejecutar si hay registros válidos
                            if (registrosValidosEnLote.Count > 0)
                            {
                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros
                                for (int i = 0; i < registrosValidosEnLote.Count; i++)
                                {
                                    var separados = registrosValidosEnLote[i];

                                    cmd.Parameters.AddWithValue($"@cajasmovimientosid{i}", separados.cajasmovimientosid.ToString());
                                    cmd.Parameters.AddWithValue($"@documentofactura{i}", separados.documentofactura);
                                }

                                /*Console.WriteLine("====== SQL GENERADO ======");
                                Console.WriteLine(cmd.CommandText);
                                Console.WriteLine("====== PARÁMETROS ======");
                                foreach (NpgsqlParameter p in cmd.Parameters)
                                {
                                    Console.WriteLine($"{p.ParameterName} = {p.Value}");
                                }
                                Console.WriteLine("==========================");
                                */
                                cmd.ExecuteNonQuery();
                                tx.Commit();
                                registrosInsertados += registrosValidosEnLote.Count;
                                Console.WriteLine($"✓ Lote insertado: {registrosValidosEnLote.Count} registros");
                            }
                            else
                            {
                                tx.Rollback();
                                Console.WriteLine("⚠ Lote omitido: no hay registros válidos");
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarClientes(List<t_cliente> Clien_list, List<t_ciudad> ciudade_lis, int batchSize = 2000)
        {
            try
            {
                if (Clien_list == null || !Clien_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Diccionarios en memoria
                    var tiposdedocumento = new Dictionary<string, int>();
                    var municipio = new Dictionary<(string codigodepartamento, string codigo), int>();
                    var departamento = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT Id, Codigo FROM public.tiposdedocumento", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);

                            tiposdedocumento[codigo] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT codigodepartamento,codigo,id FROM public.municipios", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigodepartamento = reader.GetString(0);
                            string codigo = reader.GetString(1);
                            int id = reader.GetInt32(2);


                            municipio[(codigodepartamento, codigo)] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT codigo,id FROM public.departamentos", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {

                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);


                            departamento[codigo] = id;
                        }
                    }


                    //SELECT "Id", "Categoria"

                    // Procesar en lotes
                    while (contador < Clien_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO clientes (
                                 idtiposdepersona, idtiposdedocumento, numerodedocumento, digitodeverificacion, 
                                 razonsocial, direccion, emails, telefonos, idmunicipios, papellido, 
                                 sapellido, pnombre, snombre, estado, iddepartamento
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < Clien_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = Clien_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        
                                        @tipoPersona{contador},
                                        @codigoDoc{contador},
                                        @numeroDoc{contador},
                                        @dv{contador},
                                        @razonSocial{contador},
                                        @direccion{contador},
                                        @email{contador},
                                        @telefono{contador},
                                        @municipio{contador},
                                        @apl1{contador},
                                        @apl2{contador},
                                        @nom1{contador},
                                        @nom2{contador},
                                        @estado{contador},
                                        @departamento{contador}


                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = Clien_list[batchStartIndex + i];

                                    string user = "CARLOSM";
                                    if (movi == null) continue;


                                    //cmd.Parameters.AddWithValue($"@IdCategoria{batchStartIndex + i}",
                                    // categoria.ContainsKey((movi.IdCategoria,movi.scat_ppe,movi.sbcat_ppe))
                                    //     ? categoria[(movi.IdCategoria, movi.scat_ppe, movi.sbcat_ppe)]
                                    //     : (object)DBNull.Value);



                                    var ubicacion = ciudade_lis.FirstOrDefault(c => c.Dane == movi.Dane);
                                    string parte1 = "";
                                    string parte2 = "";
                                    string xDepartamento = "";
                                    string xMunicipio = "";
                                    int xTipoPersona = 0;


                                    if (movi.tipo_per == "Natural")
                                    {
                                        xTipoPersona = 1;
                                    }
                                    else
                                    {
                                        xTipoPersona = 2;
                                    }


                                    if (ubicacion != null)
                                    {
                                        parte1 = ubicacion.Dane.Substring(0, 2);
                                        parte2 = ubicacion.Dane.Substring(2, 3);
                                        xDepartamento = ubicacion.Departamento;
                                        xMunicipio = ubicacion.Municipio;
                                    }
                                    int paramIndex = batchStartIndex + i;

                                    // Tipo Persona
                                    cmd.Parameters.AddWithValue($"@tipoPersona{paramIndex}", xTipoPersona);


                                    string key = movi.tdoc.ToString();

                                    if (key == "0")
                                        key = "13";

                                    if (!tiposdedocumento.TryGetValue(key, out var idDoc))
                                    {
                                        // Aquí loggeas el valor que NO se encontró
                                        Console.WriteLine($"[ERROR] tiposdedocumento no contiene la clave: '{key}'");

                                        // O también puedes guardar en un archivo
                                        File.AppendAllText("c:\\tmp_archivos\\diccionario_errores.txt",
                                            $"No se encontró la clave: {key}\n");

                                        // Y pones el parámetro como NULL
                                        cmd.Parameters.AddWithValue($"@codigoDoc{paramIndex}", DBNull.Value);
                                    }
                                    else
                                    {
                                        cmd.Parameters.AddWithValue($"@codigoDoc{paramIndex}", idDoc);
                                    }

                                    // Documento
                                    //cmd.Parameters.AddWithValue($"@codigoDoc{paramIndex}",
                                    //    tiposdedocumento.TryGetValue(movi.tdoc.ToString(), out var idDoc)
                                    //        ? idDoc
                                    //        : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@numeroDoc{paramIndex}",
                                        BigInteger.Parse(movi.anexo));

                                    cmd.Parameters.AddWithValue($"@dv{paramIndex}",
                                        string.IsNullOrWhiteSpace(movi.dv) ? DBNull.Value : BigInteger.Parse(movi.dv));

                                    // Nombre / Razón social
                                    cmd.Parameters.AddWithValue($"@razonSocial{paramIndex}",
                                        string.IsNullOrWhiteSpace(movi.nombre) ? DBNull.Value : movi.nombre);

                                    // Dirección
                                    cmd.Parameters.AddWithValue($"@direccion{paramIndex}",
                                        string.IsNullOrWhiteSpace(movi.direcc) ? DBNull.Value : movi.direcc);

                                    // Email
                                    cmd.Parameters.AddWithValue($"@email{paramIndex}",
                                        string.IsNullOrWhiteSpace(movi.emailfe1) ? DBNull.Value : movi.emailfe1);

                                    // Teléfono
                                    cmd.Parameters.AddWithValue($"@telefono{paramIndex}",
                                        string.IsNullOrWhiteSpace(movi.tel) ? DBNull.Value : movi.tel);

                                    // Apellidos y nombres
                                    cmd.Parameters.AddWithValue($"@apl1{paramIndex}", string.IsNullOrWhiteSpace(movi.apl1) ? DBNull.Value : movi.apl1);
                                    cmd.Parameters.AddWithValue($"@apl2{paramIndex}", string.IsNullOrWhiteSpace(movi.apl2) ? DBNull.Value : movi.apl2);
                                    cmd.Parameters.AddWithValue($"@nom1{paramIndex}", string.IsNullOrWhiteSpace(movi.nom1) ? DBNull.Value : movi.nom1);
                                    cmd.Parameters.AddWithValue($"@nom2{paramIndex}", string.IsNullOrWhiteSpace(movi.nom2) ? DBNull.Value : movi.nom2);

                                    // Estado
                                    cmd.Parameters.AddWithValue($"@estado{paramIndex}", movi.bloqueado == 1 ? false : true);

                                    // Departamento
                                    cmd.Parameters.AddWithValue($"@departamento{paramIndex}",
                                        departamento.TryGetValue(parte1, out var idDep)
                                            ? idDep
                                            : (object)DBNull.Value);

                                    // Municipio
                                    cmd.Parameters.AddWithValue($"@municipio{paramIndex}",
                                        municipio.TryGetValue((parte1, parte2), out var idMun)
                                            ? idMun
                                            : (object)DBNull.Value);

                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{Clien_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        //public static void InsertarClientes(List<t_cliente> Clien_list, List<t_ciudad> ciudade_lis)
        //{
        //    var cronometer = Stopwatch.StartNew();
        //    Stopwatch sw = new Stopwatch();
        //    sw.Start();
        //    int xTipoPersona = 0;
        //    string xDepartamento = "";
        //    string xMunicipio = "";

        //    int contador = 0; // Contador

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        // Iniciar transacción
        //        using (var tx = conn.BeginTransaction())
        //        using (var cmd = new NpgsqlCommand())
        //        {
        //            cmd.Connection = conn;
        //            cmd.Transaction = tx;

        //            // 1. Ejecutar TRUNCATE con CASCADE antes de insertar
        //            //cmd.CommandText = "TRUNCATE TABLE clientes CASCADE;";
        //            //cmd.ExecuteNonQuery();



        //            foreach (var clientes in Clien_list)
        //            {
        //                try
        //                {
        //                    cmd.Parameters.Clear(); // ← CRUCIA

        //                    // Comando SQL con parámetros
        //                    cmd.CommandText = @"
        //               INSERT INTO clientes (
        //                idtiposdepersona, idtiposdedocumento, numerodedocumento, digitodeverificacion, 
        //                razonsocial, direccion, emails, telefonos, idmunicipios, papellido, 
        //                sapellido, pnombre, snombre, estado, iddepartamento
        //                )
        //                VALUES (
        //                @tipoPersona,
        //                (SELECT id FROM tiposdedocumento WHERE codigo = @codigoDoc LIMIT 1),
        //                @numeroDoc,
        //                @dv,
        //                @razonSocial,
        //                @direccion,
        //                @email,
        //                @telefono,
        //                (SELECT id FROM municipios WHERE nombre = @municipio LIMIT 1),
        //                @apl1,
        //                @apl2,
        //                @nom1,
        //                @nom2,
        //                @estado,
        //                (SELECT id FROM departamentos WHERE nombre = @departamento LIMIT 1)
        //                );";

        //                    if (clientes.tipo_per == "Natural")
        //                    {
        //                        xTipoPersona = 1;
        //                    }
        //                    else
        //                    {
        //                        xTipoPersona = 2;
        //                    }


        //                    var ubicacion = ciudade_lis.FirstOrDefault(c => c.Dane == clientes.Dane);


        //                    if (ubicacion != null)
        //                    {
        //                        xDepartamento = ubicacion.Departamento;
        //                        xMunicipio = ubicacion.Municipio;
        //                    }

        //                    cmd.Parameters.AddWithValue("tipoPersona", xTipoPersona);
        //                    cmd.Parameters.AddWithValue("codigoDoc", string.IsNullOrWhiteSpace(clientes.tdoc) ? DBNull.Value : clientes.tdoc);
        //                    cmd.Parameters.AddWithValue("numeroDoc", BigInteger.Parse(clientes.anexo));
        //                    cmd.Parameters.AddWithValue("dv", string.IsNullOrWhiteSpace(clientes.dv) ? DBNull.Value : BigInteger.Parse(clientes.dv));
        //                    cmd.Parameters.AddWithValue("razonSocial", string.IsNullOrWhiteSpace(clientes.nombre) ? DBNull.Value : clientes.nombre);
        //                    cmd.Parameters.AddWithValue("direccion", string.IsNullOrWhiteSpace(clientes.direcc) ? DBNull.Value : clientes.direcc);
        //                    cmd.Parameters.AddWithValue("email", string.IsNullOrWhiteSpace(clientes.emailfe1) ? DBNull.Value : clientes.emailfe1);
        //                    cmd.Parameters.AddWithValue("telefono", string.IsNullOrWhiteSpace(clientes.tel) ? DBNull.Value : clientes.tel);
        //                    cmd.Parameters.AddWithValue("municipio", string.IsNullOrWhiteSpace(xMunicipio) ? DBNull.Value : xMunicipio);
        //                    cmd.Parameters.AddWithValue("apl1", string.IsNullOrWhiteSpace(clientes.apl1) ? DBNull.Value : clientes.apl1);
        //                    cmd.Parameters.AddWithValue("apl2", string.IsNullOrWhiteSpace(clientes.apl2) ? DBNull.Value : clientes.apl2);
        //                    cmd.Parameters.AddWithValue("nom1", string.IsNullOrWhiteSpace(clientes.nom1) ? DBNull.Value : clientes.nom1);
        //                    cmd.Parameters.AddWithValue("nom2", string.IsNullOrWhiteSpace(clientes.nom2) ? DBNull.Value : clientes.nom2);
        //                    cmd.Parameters.AddWithValue("estado", clientes.bloqueado == 1 ? false : true);
        //                    cmd.Parameters.AddWithValue("departamento", string.IsNullOrWhiteSpace(xDepartamento) ? DBNull.Value : xDepartamento);

        //                    // Ejecutar el comando de inserción
        //                    cmd.ExecuteNonQuery();



        //                    contador++; // Incrementar contador
        //                    Console.WriteLine($"Cliente insertado #{contador} - Documento: {clientes.anexo}"); // Mostrar en consola
        //                }
        //                catch (Exception ex)
        //                {
        //                    Console.WriteLine($"⚠️ Error al insertar cliente con documento {clientes.anexo}: {ex.Message}");
        //                }
        //            }

        //            // Confirmar la transacción
        //            tx.Commit();
        //        }
        //    }

        //    Console.WriteLine("Datos insertados correctamente.");
            
        //    TimeSpan tiempoTranscurrido = cronometer.Elapsed;
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
        //    Console.ReadKey();

        //}

        public static void InsertarMoviMecapesos(List<t_mpmovi> mpmovi_list, int batchSize = 100)
        {
            int contador = 0; // Contador
            var cronometer = Stopwatch.StartNew();
            Stopwatch sw = new Stopwatch();
            sw.Start();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Close();
                conn.Open();

                while (contador < mpmovi_list.Count)
                {
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = new NpgsqlCommand())
                    {
                        try
                        {
                            cmd.Connection = conn;
                            cmd.Transaction = tx;

                            var commandText = new StringBuilder();

                            commandText.AppendLine(@"INSERT INTO public.""MovimientoMercaPesos""(
                           ""IdTercero"", ""FechaMovimiento"", ""Factura"", ""IdSucursal"", ""IdCaja"", ""Monto"", ""IdCajero"")VALUES");

                            var valuesList = new List<string>();
                            int currentBatchSize = 0;

                            while (contador < mpmovi_list.Count && currentBatchSize < batchSize)
                            {
                                Console.WriteLine("Procesando registro " + contador);
                                var mov = mpmovi_list[contador];


                                var values = $@"
                     (
                         (SELECT DISTINCT ""Id"" FROM ""Terceros"" WHERE ""Identificacion"" = @IdTercero{contador} LIMIT 1),
                         @FechaMovimiento{contador},  
                         @Factura{contador},
                         (SELECT DISTINCT id FROM ""Sucursales"" WHERE codigo = @IdSucursal{contador} LIMIT 1),
                         @IdCaja{contador},
                         @Monto{contador},
                         @IdCajero{contador}
                     )";

                                valuesList.Add(values);
                                contador++;
                                currentBatchSize++;
                            }

                            // Combina todos los VALUES en una sola consulta
                            commandText.Append(string.Join(",", valuesList));

                            // Asigna el comando completo
                            cmd.CommandText = commandText.ToString();

                            // Agregar parámetros para el lote actual
                            for (int i = 0; i < currentBatchSize; i++)
                            {
                                var mpmovi = mpmovi_list[contador - currentBatchSize + i];

                                cmd.Parameters.AddWithValue($"@IdTercero{contador - currentBatchSize + i}", mpmovi.cedula);
                                cmd.Parameters.AddWithValue($"@FechaMovimiento{contador - currentBatchSize + i}", mpmovi.fecha);
                                cmd.Parameters.AddWithValue($"@Factura{contador - currentBatchSize + i}", DBNull.Value);
                                cmd.Parameters.AddWithValue($"@IdSucursal{contador - currentBatchSize + i}", mpmovi.sucursal);
                                cmd.Parameters.AddWithValue($"@IdCaja{contador - currentBatchSize + i}", DBNull.Value);
                                cmd.Parameters.AddWithValue($"@Monto{contador - currentBatchSize + i}", mpmovi.entra);
                                cmd.Parameters.AddWithValue($"@IdCajero{contador - currentBatchSize + i}", DBNull.Value);

                            }
                            // Ejecutar el comando de inserción
                            cmd.ExecuteNonQuery();

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar Movimiento Mercapeso: {ex.InnerException}");
                        }
                        // Confirmar la transacción
                        tx.Commit();
                    }

                }


                conn.Close();
            }
            cronometer.Stop();
            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
            Console.WriteLine("Datos insertados correctamente.");
        }

        //public static void InsertarBonoDevol(List<t_bonodev> bonodev_lst, int batchSize = 100)
        //{
        //    int contador = 0; //Contador
        //    var cronometer = Stopwatch.StartNew();

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();
        //        while (contador < bonodev_lst.Count)
        //        {
        //            using var tx = conn.BeginTransaction();
        //            using (var cmd = new NpgsqlCommand())
        //            {
        //                try
        //                {
        //                    cmd.Connection = conn;
        //                    cmd.Transaction = tx;

        //                    var commandText = new StringBuilder();

        //                    commandText.AppendLine(@"
        //                 INSERT INTO ""BonosDevolucion"" (
			     //           ""Fecha"", ""IdSucursal"", ""Numero"", ""Valor"", ""IdTercero"", ""IdSucursalDevolucion"", ""IdDocumentoDevolucion"",
			     //           ""ConsecutivoDevolucion"", ""NumeroDevolucion"", ""Concepto"", ""FechaRegistro"", ""IdUsuarioRegistro"", ""IdSucursalPOS"",
			     //           ""IdDocumentoPOS"", ""FechaPOS"", ""Clave"", ""NumeroPOS"", ""Estado"", ""CodigoBono""
        //                 ) 
        //                 VALUES");

        //                    var valuesList = new List<string>();
        //                    int currentBatchSize = 0;

        //                    while (contador < bonodev_lst.Count && currentBatchSize < batchSize)
        //                    {
        //                        Console.WriteLine("Procesando registro " + contador);
        //                        var mov = bonodev_lst[contador];


        //                        var values = $@"
        //                     (
        //                        @fecha{contador},	@super{contador},	@num_bono{contador}, @valorbono{contador}, @tercero{contador}, @suc_dev{contador}, 
        //                        (SELECT DISTINCT ""Id"" FROM ""Documentos"" WHERE ""Codigo"" = @doc_dev{contador} LIMIT 1),
			     //               @cons_dev{contador}, @num_dev{contador}, @concepto{contador}, @fech_reg{contador},
        //                        (SELECT DISTINCT ""Id"" FROM ""AspNetUsers"" WHERE ""UserName"" = @usua_reg{contador} LIMIT 1),
			     //               @suc_pos{contador}, 
        //                        (SELECT DISTINCT ""Id"" FROM ""Documentos"" WHERE ""Codigo"" = @doc_pos{contador} LIMIT 1), 
        //                        @fech_pos{contador}, @clave{contador}, @num_pos{contador}, @estado{contador}, @num_bono{contador}
        //                     )";

        //                        valuesList.Add(values);
        //                        contador++;
        //                        currentBatchSize++;
        //                    }

        //                    // Combina todos los VALUES en una sola consulta
        //                    commandText.Append(string.Join(",", valuesList));

        //                    // Asigna el comando completo
        //                    cmd.CommandText = commandText.ToString();

        //                    // Agregar parámetros para el lote actual
        //                    for (int i = 0; i < currentBatchSize; i++)
        //                    {
        //                        var mpmovi = bonodev_lst[contador - currentBatchSize + i];

        //                        cmd.Parameters.AddWithValue($"@fecha{contador - currentBatchSize + i}", new DateTime(
        //                            mpmovi.fecha.Year,
        //                            mpmovi.fecha.Month,
        //                            mpmovi.fecha.Day,
        //                            mpmovi.fecha.Hour,
        //                            mpmovi.fecha.Minute,
        //                            mpmovi.fecha.Second
        //                        ));
        //                        cmd.Parameters.AddWithValue($"@super{contador - currentBatchSize + i}", Convert.ToInt32(mpmovi.super.Trim()));
        //                        cmd.Parameters.AddWithValue($"@num_bonov", mpmovi.num_bono);
        //                        cmd.Parameters.AddWithValue($"@valorbono{contador - currentBatchSize + i}", mpmovi.valorbono);
        //                        cmd.Parameters.AddWithValue($"@tercero{contador - currentBatchSize + i}", mpmovi.terceroLong);
        //                        cmd.Parameters.AddWithValue($"@suc_dev{contador - currentBatchSize + i}", Convert.ToInt32(mpmovi.suc_dev.Trim()));
        //                        //cmd.Parameters.AddWithValue("doc_dev", Convert.ToInt32(bonodevolu.doc_dev.Trim()));
        //                        cmd.Parameters.AddWithValue($"@doc_dev{contador - currentBatchSize + i}", mpmovi.doc_dev.Trim());
        //                        cmd.Parameters.AddWithValue($"@cons_dev{contador - currentBatchSize + i}", mpmovi.cons_dev.Trim());
        //                        cmd.Parameters.AddWithValue($"@num_dev{contador - currentBatchSize + i}", mpmovi.num_dev);
        //                        cmd.Parameters.AddWithValue($"@concepto{contador - currentBatchSize + i}", mpmovi.concepto.Trim());
        //                        cmd.Parameters.AddWithValue($"@fech_reg{contador - currentBatchSize + i}", new DateTime(
        //                            mpmovi.fech_reg.Year,
        //                            mpmovi.fech_reg.Month,
        //                            mpmovi.fech_reg.Day,
        //                            mpmovi.hora_reg.Hour,
        //                            mpmovi.hora_reg.Minute,
        //                            mpmovi.hora_reg.Second
        //                        ));
        //                        cmd.Parameters.AddWithValue($"@usua_reg{contador - currentBatchSize + i}", mpmovi.usua_reg.Trim());
        //                        cmd.Parameters.AddWithValue($"@suc_pos{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(mpmovi.suc_pos) ? DBNull.Value : Convert.ToInt32(mpmovi.suc_pos.Trim()));
        //                        cmd.Parameters.AddWithValue($"@doc_pos{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(mpmovi.doc_pos) ? DBNull.Value : mpmovi.doc_pos.Trim());
        //                        cmd.Parameters.AddWithValue($"@fech_pos{contador - currentBatchSize + i}", new DateTime(
        //                            mpmovi.fech_pos.Year,
        //                            mpmovi.fech_pos.Month,
        //                            mpmovi.fech_pos.Day,
        //                            mpmovi.fech_pos.Hour,
        //                            mpmovi.fech_pos.Minute,
        //                            mpmovi.fech_pos.Second
        //                        ));
        //                        cmd.Parameters.AddWithValue($"@clave{contador - currentBatchSize + i}", mpmovi.clave.Trim());
        //                        cmd.Parameters.AddWithValue($"@num_pos{contador - currentBatchSize + i}", mpmovi.num_pos == 0 ? DBNull.Value : mpmovi.num_pos);
        //                        cmd.Parameters.AddWithValue($"@estado{contador - currentBatchSize + i}", mpmovi.estado);
        //                        cmd.Parameters.AddWithValue($"@num_bono{contador - currentBatchSize + i}", mpmovi.num_bono);

        //                    }
        //                    // Ejecutar el comando de inserción
        //                    cmd.ExecuteNonQuery();

        //                }
        //                catch (Exception ex)
        //                {
        //                    Console.WriteLine($"⚠️ Error al insertar Movimiento Mercapeso: {ex.InnerException}");
        //                }
        //                // Confirmar la transacción
        //                tx.Commit();
        //            }


        //        }

        //        conn.Close();
        //    }

        //    cronometer.Stop();
        //    TimeSpan tiempoTranscurrido = cronometer.Elapsed;
        //    Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");

        //}

        public static async Task InsertarBonoDevol(List<t_bonodev> bonodev_lst, int batchSize = 100)
        {
            try
            {
                if (bonodev_lst == null || !bonodev_lst.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Diccionarios en memoria


                    var documentos = new Dictionary<string, int>();
                    var usuarios = new Dictionary<string, string>();


                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM ""Documentos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            documentos[codigo] = id;
                        }
                    }



                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""UserName"" FROM ""AspNetUsers""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string id = reader.GetString(0);       // Id en AspNetUsers casi siempre es string
                            string username = reader.GetString(1); // UserName
                            usuarios[username] = id;
                        }
                    }


                    //SELECT "Id", "Categoria"

                    // Procesar en lotes
                    while (contador < bonodev_lst.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""BonosDevolucion""(
                                ""Fecha"", ""IdSucursal"", ""Numero"", ""Valor"", ""IdTercero"", ""IdSucursalDevolucion"", ""IdDocumentoDevolucion"",
			                    ""ConsecutivoDevolucion"", ""NumeroDevolucion"", ""Concepto"", ""FechaRegistro"", ""IdUsuarioRegistro"", ""IdSucursalPOS"",
			                    ""IdDocumentoPOS"", ""FechaPOS"", ""Clave"", ""NumeroPOS"", ""Estado"", ""CodigoBono""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < bonodev_lst.Count && currentBatchSize < batchSize)
                                {
                                    var mov = bonodev_lst[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        
                                       
                                      @fecha{contador},	@super{contador},	@num_bono{contador}, @valorbono{contador}, @tercero{contador}, @suc_dev{contador}, 
                                      @doc_dev{contador},
			                          @cons_dev{contador}, @num_dev{contador}, @concepto{contador}, @fech_reg{contador},
                                      @usua_reg{contador},
			                          @suc_pos{contador}, 
                                      @doc_pos{contador}, 
                                      @fech_pos{contador}, @clave{contador}, @num_pos{contador}, @estado{contador}, @num_bono{contador}
   

                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = bonodev_lst[batchStartIndex + i];

                                    string user = "CARLOSM";
                                    if (movi == null) continue;


                                    cmd.Parameters.AddWithValue($"@fecha{contador - currentBatchSize + i}", new DateTime(
                                        movi.fecha.Year,
                                        movi.fecha.Month,
                                        movi.fecha.Day,
                                        movi.fecha.Hour,
                                        movi.fecha.Minute,
                                        movi.fecha.Second
                                    ));
                                    cmd.Parameters.AddWithValue($"@super{contador - currentBatchSize + i}", Convert.ToInt32(movi.super.Trim()));
                                    cmd.Parameters.AddWithValue($"@num_bonov", movi.num_bono);
                                    cmd.Parameters.AddWithValue($"@valorbono{contador - currentBatchSize + i}", movi.valorbono);
                                    cmd.Parameters.AddWithValue($"@tercero{contador - currentBatchSize + i}", movi.terceroLong);
                                    cmd.Parameters.AddWithValue($"@suc_dev{contador - currentBatchSize + i}", Convert.ToInt32(movi.suc_dev.Trim()));
                                    //cmd.Parameters.AddWithValue("doc_dev", Convert.ToInt32(bonodevolu.doc_dev.Trim()));
                                    //cmd.Parameters.AddWithValue($"@doc_dev{contador - currentBatchSize + i}", movi.doc_dev.Trim());

                                    cmd.Parameters.AddWithValue($"@doc_dev{batchStartIndex + i}", documentos.TryGetValue(movi.doc_dev, out var doc) ? doc : (object)DBNull.Value);



                                    cmd.Parameters.AddWithValue($"@cons_dev{contador - currentBatchSize + i}", movi.cons_dev.Trim());
                                    cmd.Parameters.AddWithValue($"@num_dev{contador - currentBatchSize + i}", movi.num_dev);
                                    cmd.Parameters.AddWithValue($"@concepto{contador - currentBatchSize + i}", movi.concepto.Trim());
                                    cmd.Parameters.AddWithValue($"@fech_reg{contador - currentBatchSize + i}", new DateTime(
                                        movi.fech_reg.Year,
                                        movi.fech_reg.Month,
                                        movi.fech_reg.Day,
                                        movi.hora_reg.Hour,
                                        movi.hora_reg.Minute,
                                        movi.hora_reg.Second
                                    ));
                                    //cmd.Parameters.AddWithValue($"@usua_reg{contador - currentBatchSize + i}", movi.usua_reg.Trim());

                                    cmd.Parameters.AddWithValue($"@usua_reg{batchStartIndex + i}",
                                   usuarios.TryGetValue(movi.usua_reg, out var userId) ? userId : (object)DBNull.Value);


                                    cmd.Parameters.AddWithValue($"@suc_pos{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(movi.suc_pos) ? DBNull.Value : Convert.ToInt32(movi.suc_pos.Trim()));
                                    //cmd.Parameters.AddWithValue($"@doc_pos{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(movi.doc_pos) ? DBNull.Value : movi.doc_pos.Trim());

                                    cmd.Parameters.AddWithValue($"@doc_pos{batchStartIndex + i}", documentos.TryGetValue(movi.doc_pos, out var doc_pos) ? doc_pos : (object)DBNull.Value);


                                    cmd.Parameters.AddWithValue($"@fech_pos{contador - currentBatchSize + i}", new DateTime(
                                        movi.fech_pos.Year,
                                        movi.fech_pos.Month,
                                        movi.fech_pos.Day,
                                        movi.fech_pos.Hour,
                                        movi.fech_pos.Minute,
                                        movi.fech_pos.Second
                                    ));
                                    cmd.Parameters.AddWithValue($"@clave{contador - currentBatchSize + i}", movi.clave.Trim());
                                    cmd.Parameters.AddWithValue($"@num_pos{contador - currentBatchSize + i}", movi.num_pos == 0 ? DBNull.Value : movi.num_pos);
                                    cmd.Parameters.AddWithValue($"@estado{contador - currentBatchSize + i}", movi.estado);
                                    cmd.Parameters.AddWithValue($"@num_bono{contador - currentBatchSize + i}", movi.num_bono);

                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{bonodev_lst.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static void InsertarHistoNove(List<t_histonove> nove_list)
        {
            var cronometer = Stopwatch.StartNew();
            Stopwatch sw = new Stopwatch();
            sw.Start();

            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                foreach (var histonove in nove_list)
                {
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;

                        try
                        {
                            cmd.Parameters.Clear();

                            cmd.CommandText = @"
                        INSERT INTO cambioeps (
                            entidadesnominaanteriorid,
                            entidadesnominanuevaid,
                            usuarioid,
                            fechahora,
                            empleadoid,
                            reportartraslado,
                            fechacambio
                        )
                        VALUES (
                            (SELECT DISTINCT id FROM public.""EntidadesNomina"" WHERE codigo = @codAnt),
                            (SELECT DISTINCT id FROM public.""EntidadesNomina"" WHERE codigo = @codAct),
                            (SELECT DISTINCT ""Id"" FROM public.""AspNetUsers"" WHERE ""UserName"" = @userReg),
                            @fechaHora,
                            (SELECT DISTINCT ""Id"" FROM public.""Empleados"" WHERE ""CodigoEmpleado"" = @empleado),
                            @reportarTraslado,
                            @fechaCambio
                        );";

                            cmd.Parameters.AddWithValue("codAnt", histonove.cod_ant.Trim());
                            cmd.Parameters.AddWithValue("codAct", histonove.cod_act.Trim());
                            cmd.Parameters.AddWithValue("userReg", histonove.usua_reg.Trim());
                            cmd.Parameters.AddWithValue("fechaHora", new DateTime(
                                histonove.fech_reg.Year,
                                histonove.fech_reg.Month,
                                histonove.fech_reg.Day,
                                histonove.hora_reg.Hour,
                                histonove.hora_reg.Minute,
                                histonove.hora_reg.Second
                            ));
                            cmd.Parameters.AddWithValue("empleado", histonove.empleado.Trim());
                            cmd.Parameters.AddWithValue("reportarTraslado", histonove.repo_soi == 1);
                            cmd.Parameters.AddWithValue("fechaCambio", histonove.fech_camb);

                            cmd.ExecuteNonQuery();
                            conn.Close();
                            contador++;
                            Console.WriteLine($"✅ Registro insertado correctamente #{contador}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error en registro (empleado {histonove.empleado}): {ex.Message}");
                            // No se hace rollback, simplemente continúa con el siguiente
                        }
                    }
                }
            }

            Console.WriteLine("Proceso finalizado.");
            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
            Console.ReadKey();
        }

        //public static void InsertarHistonoveP(List<t_histonove> nove_pen)
        //{

        //    int contador = 0; // Contador
        //    var cronometer = Stopwatch.StartNew();
        //    Stopwatch sw = new Stopwatch();
        //    sw.Start();

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        // Iniciar transacción
        //        using (var tx = conn.BeginTransaction())
        //        using (var cmd = new NpgsqlCommand())
        //        {
        //            cmd.Connection = conn;
        //            cmd.Transaction = tx;

        //            foreach (var histonove in nove_pen)
        //            {
        //                try
        //                {
        //                    cmd.Parameters.Clear(); // ← CRUCIA

        //                    // Comando SQL con parámetros
        //                    cmd.CommandText = @"
        //                INSERT INTO cambiopension (
        //                    entidadesnominaanteriorid,
        //                    entidadesnominanuevaid,
        //                    usuarioid,
        //                    fechahora,
        //                    empleadoid,
        //                    reportartraslado,
        //                    fechacambio
        //                )
        //                VALUES (
        //                    (SELECT DISTINCT id FROM public.""EntidadesNomina"" WHERE codigo = @codAnt),
        //                    (SELECT DISTINCT id FROM public.""EntidadesNomina""WHERE codigo = @codAct),
        //                    (SELECT DISTINCT  ""Id"" FROM public.""AspNetUsers"" WHERE ""UserName"" = @userReg),
        //                    @fechaHora,
        //                    (SELECT DISTINCT ""Id"" FROM public.""Empleados"" WHERE ""CodigoEmpleado"" = @empleado),
        //                    @reportarTraslado,
        //                    @fechaCambio
        //                );";


        //                    cmd.Parameters.AddWithValue("codAnt", histonove.cod_ant.Trim());
        //                    cmd.Parameters.AddWithValue("codAct", histonove.cod_act.Trim());
        //                    cmd.Parameters.AddWithValue("userReg", histonove.usua_reg.Trim());
        //                    cmd.Parameters.AddWithValue("fechaHora", new DateTime(
        //                        histonove.fech_reg.Year,
        //                        histonove.fech_reg.Month,
        //                        histonove.fech_reg.Day,
        //                        histonove.hora_reg.Hour,
        //                        histonove.hora_reg.Minute,
        //                        histonove.hora_reg.Second
        //                    ));
        //                    cmd.Parameters.AddWithValue("empleado", histonove.empleado.Trim());
        //                    cmd.Parameters.AddWithValue("reportarTraslado", histonove.repo_soi == 1);
        //                    cmd.Parameters.AddWithValue("fechaCambio", histonove.fech_camb);

        //                    // Ejecutar el comando de inserción
        //                    cmd.ExecuteNonQuery();



        //                    contador++; // Incrementar contador
        //                    Console.WriteLine($"Cliente insertado #{contador}"); // Mostrar en consola
        //                }
        //                catch (Exception ex)
        //                {
        //                    Console.WriteLine($"⚠️ Error al insertar cliente con documento: {ex.Message}");
        //                }
        //            }

        //            // Confirmar la transacción
        //            tx.Commit();
        //        }
        //    }

        //    Console.WriteLine("Datos insertados correctamente.");
        //    TimeSpan tiempoTranscurrido = cronometer.Elapsed;
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
        //    Console.ReadKey();


        //}

        //public static void InsertarHistonoveS(List<t_histonove> nove_pen)
        //{
        //    int contador = 0; // Contador
        //    var cronometer = Stopwatch.StartNew();
        //    Stopwatch sw = new Stopwatch();
        //    sw.Start();

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        // Iniciar transacción
        //        using (var tx = conn.BeginTransaction())
        //        using (var cmd = new NpgsqlCommand())
        //        {
        //            cmd.Connection = conn;
        //            cmd.Transaction = tx;

        //            foreach (var histonove in nove_pen)
        //            {
        //                try
        //                {
        //                    cmd.Parameters.Clear(); // ← CRUCIA

        //                    // Comando SQL con parámetros
        //                    cmd.CommandText = @"
        //                INSERT INTO cambiosalario (
        //                    salarioanterior,
        //                    salarionuevo,
        //                    usuarioid,
        //                    fechahora,
        //                    empleadoid,
        //                    reportavariacion,
        //                    fechahoravariacion
        //                )
        //                VALUES (
        //                    @val_ant,
        //                    @val_act,
        //                    (SELECT DISTINCT  ""Id"" FROM public.""AspNetUsers"" WHERE ""UserName"" = @userReg),
        //                    @fechaHora,
        //                    (SELECT DISTINCT ""Id"" FROM public.""Empleados"" WHERE ""CodigoEmpleado"" = @empleado),
        //                    @reportarTraslado,
        //                    @fechaCambio
        //                );";


        //                    cmd.Parameters.AddWithValue("val_ant", histonove.val_ant);
        //                    cmd.Parameters.AddWithValue("val_act", histonove.val_act);
        //                    cmd.Parameters.AddWithValue("userReg", histonove.usua_reg.Trim());
        //                    cmd.Parameters.AddWithValue("fechaHora", new DateTime(
        //                        histonove.fech_reg.Year,
        //                        histonove.fech_reg.Month,
        //                        histonove.fech_reg.Day,
        //                        histonove.hora_reg.Hour,
        //                        histonove.hora_reg.Minute,
        //                        histonove.hora_reg.Second
        //                    ));
        //                    cmd.Parameters.AddWithValue("empleado", histonove.empleado.Trim());
        //                    cmd.Parameters.AddWithValue("reportarTraslado", histonove.repo_soi == 1);
        //                    cmd.Parameters.AddWithValue("fechaCambio", histonove.fech_camb);

        //                    // Ejecutar el comando de inserción
        //                    cmd.ExecuteNonQuery();



        //                    contador++; // Incrementar contador
        //                    Console.WriteLine($"Cliente insertado #{contador}"); // Mostrar en consola
        //                }
        //                catch (Exception ex)
        //                {
        //                    Console.WriteLine($"⚠️ Error al insertar cliente con documento: {ex.Message}");
        //                    //ex.InnerException
        //                }
        //            }

        //            // Confirmar la transacción
        //            tx.Commit();
        //        }
        //    }

        //    Console.WriteLine("Datos insertados correctamente.");
        //    TimeSpan tiempoTranscurrido = cronometer.Elapsed;
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
        //    Console.ReadKey();


        //}

        public static void InsertarHistonoveP(List<t_histonove> nove_pen)
        {
            int contador = 0;
            var cronometer = Stopwatch.StartNew();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                foreach (var histonove in nove_pen)
                {
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;

                        try
                        {
                            cmd.Parameters.Clear();

                            cmd.CommandText = @"
                        INSERT INTO cambiopension (
                            entidadesnominaanteriorid,
                            entidadesnominanuevaid,
                            usuarioid,
                            fechahora,
                            empleadoid,
                            reportartraslado,
                            fechacambio
                        )
                        VALUES (
                            (SELECT DISTINCT id FROM public.""EntidadesNomina"" WHERE codigo = @codAnt),
                            (SELECT DISTINCT id FROM public.""EntidadesNomina"" WHERE codigo = @codAct),
                            (SELECT DISTINCT ""Id"" FROM public.""AspNetUsers"" WHERE ""UserName"" = @userReg),
                            @fechaHora,
                            (SELECT DISTINCT ""Id"" FROM public.""Empleados"" WHERE ""CodigoEmpleado"" = @empleado),
                            @reportarTraslado,
                            @fechaCambio
                        );";

                            cmd.Parameters.AddWithValue("codAnt", histonove.cod_ant.Trim());
                            cmd.Parameters.AddWithValue("codAct", histonove.cod_act.Trim());
                            cmd.Parameters.AddWithValue("userReg", histonove.usua_reg.Trim());
                            cmd.Parameters.AddWithValue("fechaHora", new DateTime(
                                histonove.fech_reg.Year,
                                histonove.fech_reg.Month,
                                histonove.fech_reg.Day,
                                histonove.hora_reg.Hour,
                                histonove.hora_reg.Minute,
                                histonove.hora_reg.Second
                            ));
                            cmd.Parameters.AddWithValue("empleado", histonove.empleado.Trim());
                            cmd.Parameters.AddWithValue("reportarTraslado", histonove.repo_soi == 1);
                            cmd.Parameters.AddWithValue("fechaCambio", histonove.fech_camb);

                            cmd.ExecuteNonQuery();
                            conn.Close();

                            contador++;
                            Console.WriteLine($"✅ Registro de pensión insertado #{contador}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar pensión (empleado {histonove.empleado}): {ex.Message}");
                        }
                    }
                }
            }

            Console.WriteLine("✅ Proceso pensiones finalizado.");
            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
            Console.ReadKey();
        }

        public static void InsertarHistonoveS(List<t_histonove> nove_pen)
        {
            int contador = 0;
            var cronometer = Stopwatch.StartNew();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                foreach (var histonove in nove_pen)
                {
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;

                        try
                        {
                            cmd.Parameters.Clear();

                            cmd.CommandText = @"
                        INSERT INTO cambiosalario (
                            salarioanterior,
                            salarionuevo,
                            usuarioid,
                            fechahora,
                            empleadoid,
                            reportavariacion,
                            fechahoravariacion
                        )
                        VALUES (
                            @val_ant,
                            @val_act,
                            (SELECT DISTINCT ""Id"" FROM public.""AspNetUsers"" WHERE ""UserName"" = @userReg),
                            @fechaHora,
                            (SELECT DISTINCT ""Id"" FROM public.""Empleados"" WHERE ""CodigoEmpleado"" = @empleado),
                            @reportarTraslado,
                            @fechaCambio
                        );";

                            cmd.Parameters.AddWithValue("val_ant", histonove.val_ant);
                            cmd.Parameters.AddWithValue("val_act", histonove.val_act);
                            cmd.Parameters.AddWithValue("userReg", histonove.usua_reg.Trim());
                            cmd.Parameters.AddWithValue("fechaHora", new DateTime(
                                histonove.fech_reg.Year,
                                histonove.fech_reg.Month,
                                histonove.fech_reg.Day,
                                histonove.hora_reg.Hour,
                                histonove.hora_reg.Minute,
                                histonove.hora_reg.Second
                            ));
                            cmd.Parameters.AddWithValue("empleado", histonove.empleado.Trim());
                            cmd.Parameters.AddWithValue("reportarTraslado", histonove.repo_soi == 1);
                            cmd.Parameters.AddWithValue("fechaCambio", histonove.fech_camb);

                            cmd.ExecuteNonQuery();
                            conn.Close();
                            contador++;
                            Console.WriteLine($"✅ Registro de salario insertado #{contador}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar salario (empleado {histonove.empleado}): {ex.Message}");
                        }
                    }
                }
            }

            Console.WriteLine("✅ Proceso salarios finalizado.");
            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
            Console.ReadKey();
        }

        //public static void InsertarProductos(List<t_items> items_list)
        //{
        //    int contador = 0; // Contador
        //    var cronometer = Stopwatch.StartNew();

        //    Stopwatch sw = new Stopwatch();
        //    sw.Start();


        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        // Iniciar transacción
        //        using (var tx = conn.BeginTransaction())
        //        using (var cmd = new NpgsqlCommand())
        //        {
        //            cmd.Connection = conn;
        //            cmd.Transaction = tx;

        //            foreach (var items in items_list)
        //            {
        //                try
        //                {
        //                    cmd.Parameters.Clear(); // ← CRUCIAL

        //                    // Comando SQL con parámetros
        //cmd.CommandText = $@"
        //INSERT INTO ""Productos"" (
        //                    ""Codigo"", ""IdTipoProducto"", ""FechaCreacion"", ""Nombre"", ""NombreCorto"", ""ReferenciaFabrica"",
        //                    ""PesoUnitario"", ""UnidadesPorCaja"", ""IdCategoria"", ""IdSubgruposProductos"", ""IdMarcasProductos"",
        //                    ""PesoDrenado"", ""Ean8"", ""Ean13"", ""Estado"", ""Unidad"", ""Talla"", ""IdLineasProductos"", ""IdSublineasProductos"",
        //                    ""NoAutorizar"", ""Inactivo"", ""EsKIT"", ""Iva"", ""Precio"", ""PrecioAnterior"", ""PrecioCambio"", ""Rentabilidad"",
        //                    ""BaseImpuestoConsumo"", ""IvaExento"", ""ImpuestoConsumo"", ""IdGrupoContable"", ""FactorImpuestoConsumo"",
        //                    ""IdRetFuente"", ""CostoAjuste"", ""Fruver"", ""BolsaAgro"", ""ModificaPrecio"", ""Fenaice"", ""AcumulaTira"",
        //                    ""SubsidioDesempleo"", ""ModificaCantidad"", ""BotFut"", ""ContabilizarGrabado"", ""NoAutorizaCompra"",
        //                    ""BalanzaToledo"", ""PrefijoEAN"", ""DescuentoAsohofrucol"", ""Motocicleta"", ""Pedido"", ""Codensa"", ""Ingreso"",
        //                    ""CheckeoPrecio"", ""IdEvento"", ""Tipo"", ""AdmiteDescuento"", ""DescuentoMaximo"", ""Decimales"", ""FechaCambio"",
        //                    ""CostoReposicion"", ""FenaicePorcentaje"", ""BotFutPorcentaje"", ""PrefijoEANPorcentaje"", ""CodensaPorcentaje"",
        //                    ""CodigoAlterno"",""CCosto"",""ExcluirDeListado"",""PermitirSaldoEnRojo"",""ExigirTarjetMercapesos"",""Corrosivo"",
        //                    ""CAS"",""CategoriaCAS"",""PermitirVentaPorDebajoCosto"",""DetalleAlElaborarDocumento"",""UnicamenteManejarCantidad"",
        //                    ""SolicitarSerieAlFacturar"",""TerceroAutomaticoPOS"",""ProductoGenerico"",""PesoBruto"",""PesoNeto"",""PesoTara"",
        //                    ""UnidadMinimaDeVenta"",""Factor"",""InicioMercapesos"",""FinalMercapesos"",""DescuentoEspecial"",
        //                    ""FactorDescuento"",""ValorDescuentoFijo"",""InicioDescuento"",""FinalDescuento"",""DecodificarProducto"",
        //                    ""DiaDecodificacion"",""DiaUsuarioDecodificacion"",""ValidarMaxVentasPorTercero"",""UnidadMaximaVentas"",""NoIncluirDespacho"",""FechaBloqueoPedido"",
        //                    ""TipoControl"",""DiasSiguientesCompra"",""PrecioVenta3"",""NoIncluirEnInventarioParaPedido"",""AutorizarTrasladoProducto"",
        //                    ""PrecioControlVingilancia"",""NoIncluirreporteChequeo"",""NoCalcularCostoPromedio"",""ProductoSobreStock"",""EnviarAAsopanela"",""UnidadArticulo"",
        //                    ""GrupoDescuento"",""CodigoEan1"",""CodigoEan2"",""ProductoInAndOut"",""DomiciliosCom"",""PesoPOS"",
        //                    ""OrdenadoInicioFactura"",""ModificarConTolerancia"",""PorcentajeTolerancia"",""Stock"",""ExcluidoCOVID"",""IdUnidadDIAN"",
        //                    ""IdTipoBienDIAN"",""DiasSinIVA"",""DescuentoFNFP"",""IdVariedadFNFP"",""DescuentoNIIF"",""NoUtilizar"",
        //                    ""FactorImpuestoConsumoRest"",""IdBodega"",""IdUnidadProducto"",""PrecioIva"",""ValorIva"",""CostoAjusteNIIF"",
        //                    ""DescuentoPorcentaje"",""RentabilidadSugerida"",""ConfirmarCambioPrecio"",""ProductoOfertado"",""FechaPrimerMovimiento"",""TipoImpuestoAlimentos"",
        //                    ""ValorTipoImpuestoAlimentos"",""GeneraImpuestoSaludable"",""CodigoReferencia"",""Referencia""
        //                    ) VALUES(
        //                     @Codigo, @IdTipoProducto, @FechaCreacion, @Nombre, @NombreCorto, @ReferenciaFabrica,
        //                     @PesoUnitario, @UnidadesPorCaja, @IdCategoria,
        //                     (SELECT DISTINCT ""id"" FROM ""SubgruposProductos"" WHERE Codigo = @subgrupo LIMIT 1),
        //                     (SELECT DISTINCT ""id"" FROM ""MarcasProductos"" WHERE Codigo = @marca LIMIT 1) ,
        //                     @PesoDrenado, @Ean8, @Ean13, @Estado,@Unidad,@Talla,
        //                     (SELECT DISTINCT ""id"" FROM ""LineasProductos"" WHERE Codigo = @linea LIMIT 1),
        //                     (SELECT DISTINCT ""id"" FROM ""SublineasProductos"" WHERE Codigo = @sublinea LIMIT 1),
        //                     @NoAutorizar, @Inactivo,@EsKIT, @Iva, @Precio,@PrecioAnterior,@PrecioCambio,@Rentabilidad,
        //                     @BaseImpuestoConsumo,@IvaExento,@ImpuestoConsumo,@IdGrupoContable,@FactorImpuestoConsumo,
        //                     @IdRetFuente,@CostoAjuste, @Fruver,@BolsaAgro,@ModificaPrecio,@Fenaice,@AcumulaTira,
        //                     @SubsidioDesempleo, @ModificaCantidad, @BotFut, @ContabilizarGrabado, @NoAutorizaCompra,
        //                     @BalanzaToledo, @PrefijoEAN, @DescuentoAsohofrucol, @Motocicleta, @Pedido, @Codensa, @Ingreso,
        //                     @CheckeoPrecio, @IdEvento, @Tipo, @AdmiteDescuento, @DescuentoMaximo, @Decimales, @FechaCambio,
        //                     @CostoReposicion, @FenaicePorcentaje, @BotFutPorcentaje, @PrefijoEANPorcentaje, @CodensaPorcentaje,
        //                     @CodigoAlterno,@CCosto,@ExcluirDeListado,@PermitirSaldoEnRojo,@ExigirTarjetMercapesos,@Corrosivo,
        //                     @CAS,@CategoriaCAS,@PermitirVentaPorDebajoCosto,@DetalleAlElaborarDocumento,@UnicamenteManejarCantidad,
        //                     @SolicitarSerieAlFacturar,@TerceroAutomaticoPOS,@ProductoGenerico,@PesoBruto,@PesoNeto,@PesoTara,
        //                     @UnidadMinimaDeVenta,@Factor,@InicioMercapesos,@FinalMercapesos,@DescuentoEspecial,
        //                     @FactorDescuento,@ValorDescuentoFijo,@InicioDescuento,@FinalDescuento,@DecodificarProducto,
        //                     @DiaDecodificacion,@DiaUsuarioDecodificacion,@ValidarMaxVentasPorTercero,@UnidadMaximaVentas,@NoIncluirDespacho,@FechaBloqueoPedido,
        //                     @TipoControl,@DiasSiguientesCompra,@PrecioVenta3,@NoIncluirEnInventarioParaPedido,@AutorizarTrasladoProducto,
        //                     @PrecioControlVingilancia,@NoIncluirreporteChequeo,@NoCalcularCostoPromedio,@ProductoSobreStock,@EnviarAAsopanela,@UnidadArticulo,
        //                     @GrupoDescuento,@CodigoEan1,@CodigoEan2,@ProductoInAndOut,@DomiciliosCom,@PesoPOS,
        //                     @OrdenadoInicioFactura,@ModificarConTolerancia,@PorcentajeTolerancia,@Stock,@ExcluidoCOVID,
        //                     (SELECT DISTINCT ""Id"" FROM ""UnidadesDIAN"" WHERE ""Codigo"" = @unidcantfe LIMIT 1),
        //                     (SELECT DISTINCT ""Id"" FROM ""TipoBienDIAN"" WHERE ""Codigo"" = @tipobiend LIMIT 1),
        //                     @DiasSinIVA, @DescuentoFNFP, @IdVariedadFNFP, @DescuentoNIIF, @NoUtilizar,
        //                     @FactorImpuestoConsumoRest,
        //                     (SELECT DISTINCT id FROM bodegas WHERE Codigo = @bod_asoc LIMIT 1),
        //                     (SELECT DISTINCT id FROM unidaddemedida WHERE descripcion = @unidad LIMIT 1),
        //                     @PrecioIva, @ValorIva, @CostoAjusteNIIF,
        //                     @DescuentoPorcentaje,
        //                     @RentabilidadSugerida, @ConfirmarCambioPrecio, @ProductoOfertado, @FechaPrimerMovimiento, @TipoImpuestoAlimentos,
        //                     @ValorTipoImpuestoAlimentos, @GeneraImpuestoSaludable, @CodigoReferencia, @Referencia
        //                    );";

        //                    string shortnm = items.shortname?.Length > 50 ? items.shortname.Substring(0, 50) : items.shortname;

        //                    cmd.Parameters.AddWithValue("@Codigo", items.Codigo);
        //                    cmd.Parameters.AddWithValue("@IdTipoProducto", int.Parse(items.tipo));
        //                    cmd.Parameters.AddWithValue("@FechaCreacion", items.fecha_cre);
        //                    cmd.Parameters.AddWithValue("@Nombre", items.nombre);
        //                    cmd.Parameters.AddWithValue("@NombreCorto", shortnm);
        //                    cmd.Parameters.AddWithValue("@ReferenciaFabrica", items.refabrica);
        //                    cmd.Parameters.AddWithValue("@PesoUnitario", items.peso_uni);
        //                    cmd.Parameters.AddWithValue("@UnidadesPorCaja", items.undxcaja);
        //                    cmd.Parameters.AddWithValue("@IdCategoria", 0);
        //                    cmd.Parameters.AddWithValue("@subgrupo", items.subgrupo);
        //                    cmd.Parameters.AddWithValue("@marca", items.marca);
        //                    cmd.Parameters.AddWithValue("@PesoDrenado", items.pdrenado);
        //                    cmd.Parameters.AddWithValue("@Ean8", items.cod_ean8);
        //                    cmd.Parameters.AddWithValue("@Ean13", items.cod_bar);
        //                    cmd.Parameters.AddWithValue("@Estado", items.bloqueado == 1 ? false : true);
        //                    cmd.Parameters.AddWithValue("@Unidad", items.unidad);
        //                    cmd.Parameters.AddWithValue("@Talla", items.talla);
        //                    cmd.Parameters.AddWithValue("@linea", items.linea);
        //                    cmd.Parameters.AddWithValue("@sublinea", items.sublinea);
        //                    cmd.Parameters.AddWithValue("@NoAutorizar", items.no_compra);
        //                    cmd.Parameters.AddWithValue("@Inactivo", items.bloqueado);
        //                    cmd.Parameters.AddWithValue("@EsKIT", items.es_kitpro ==1 ? false:true);
        //                    cmd.Parameters.AddWithValue("@Iva", items.iva);
        //                    cmd.Parameters.AddWithValue("@Precio", items.pvtali);
        //                    cmd.Parameters.AddWithValue("@PrecioAnterior", items.pvta_a1);
        //                    cmd.Parameters.AddWithValue("@PrecioCambio", items.cambiopv_1);
        //                    cmd.Parameters.AddWithValue("@Rentabilidad", CalcularRentabilidad(items.pvta1i, items.costo_rep, 3, items.iva, items.iconsumo, items.listap, items.imp_salu, items.vr_imps, DateTime.Now , items.gen_impu));
        //                    cmd.Parameters.AddWithValue("@BaseImpuestoConsumo", items.iconsumo > 0 ? true : false);
        //                    cmd.Parameters.AddWithValue("@IvaExento", items.excluido == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ImpuestoConsumo", items.listap);
        //                    cmd.Parameters.AddWithValue("@IdGrupoContable", 0);
        //                    cmd.Parameters.AddWithValue("@FactorImpuestoConsumo", items.F_ICONSUMO);
        //                    cmd.Parameters.AddWithValue("@IdRetFuente", 0);
        //                    cmd.Parameters.AddWithValue("@CostoAjuste", items.costoajus);
        //                    cmd.Parameters.AddWithValue("@Fruver", items.es_fruver == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@BolsaAgro", items.bolsa == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ModificaPrecio", items.mod_ppos == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@Fenaice", string.IsNullOrWhiteSpace(items.fenalce) ? true : false);
        //                    cmd.Parameters.AddWithValue("@AcumulaTira", items.acu_tpos == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@SubsidioDesempleo", items.subsidio == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ModificaCantidad", items.mod_qpos == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@BotFut", items.es_bol == "1" ? true : false);
        //                    cmd.Parameters.AddWithValue("@ContabilizarGrabado", items.contabgrav == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@NoAutorizaCompra", items.no_compra == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@BalanzaToledo", items.sitoledo == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@PrefijoEAN", string.IsNullOrWhiteSpace(items.pref_ean) ? true : false);
        //                    cmd.Parameters.AddWithValue("@DescuentoAsohofrucol", items.es_bordado ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@Motocicleta", items.es_moto == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@Pedido", items.sipedido == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@Codensa", items.escodensa == "S" ? true : false);
        //                    cmd.Parameters.AddWithValue("@Ingreso", items.es_ingreso ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@CheckeoPrecio", items.cheqpr == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@IdEvento", 0);
        //                    cmd.Parameters.AddWithValue("@Tipo", short.Parse(items.tipo));
        //                    cmd.Parameters.AddWithValue("@AdmiteDescuento", items.si_descto == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@DescuentoMaximo", items.descmax);
        //                    cmd.Parameters.AddWithValue("@Decimales", items.deci_cant);
        //                    cmd.Parameters.AddWithValue("@FechaCambio", items.fech_cp1);
        //                    cmd.Parameters.AddWithValue("@CostoReposicion", items.costo_rep);
        //                    cmd.Parameters.AddWithValue("@FenaicePorcentaje", 0);
        //                    cmd.Parameters.AddWithValue("@BotFutPorcentaje", 0);
        //                    cmd.Parameters.AddWithValue("@PrefijoEANPorcentaje", string.IsNullOrWhiteSpace(items.pref_ean)?0: int.Parse(items.pref_ean));
        //                    cmd.Parameters.AddWithValue("@CodensaPorcentaje", 0);
        //                    cmd.Parameters.AddWithValue("@CodigoAlterno", items.cod_alt);
        //                    cmd.Parameters.AddWithValue("@CCosto",string.IsNullOrWhiteSpace(items.CCosto)?DBNull.Value:int.Parse(items.CCosto));
        //                    cmd.Parameters.AddWithValue("@ExcluirDeListado", items.elegido == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@PermitirSaldoEnRojo", items.sdo_rojo == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ExigirTarjetMercapesos", items.pidempeso == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@Corrosivo", items.corrosivo == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@CAS", string.IsNullOrWhiteSpace(items.n_cas)?DBNull.Value: items.n_cas);
        //                    cmd.Parameters.AddWithValue("@CategoriaCAS", string.IsNullOrWhiteSpace(items.cat_cas) ? DBNull.Value : items.cat_cas);
        //                    cmd.Parameters.AddWithValue("@PermitirVentaPorDebajoCosto", items.vta_costo == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@DetalleAlElaborarDocumento", items.si_detdoc == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@UnicamenteManejarCantidad", items.solocant == 1 ? true :false);
        //                    cmd.Parameters.AddWithValue("@SolicitarSerieAlFacturar", items.si_serie == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@TerceroAutomaticoPOS", items.terceAutom == 1 ? true :false);
        //                    cmd.Parameters.AddWithValue("@ProductoGenerico", items.generico == 1 ?true : false);
        //                    cmd.Parameters.AddWithValue("@PesoBruto", items.pesocajb);
        //                    cmd.Parameters.AddWithValue("@PesoNeto", items.pesocajn);
        //                    cmd.Parameters.AddWithValue("@PesoTara", items.peso_car);
        //                    cmd.Parameters.AddWithValue("@UnidadMinimaDeVenta", items.unidmin);
        //                    cmd.Parameters.AddWithValue("@Factor", items.puntaje);
        //                    cmd.Parameters.AddWithValue("@InicioMercapesos", items.fecha1_mp == null ? DBNull.Value:items.fecha1_mp);
        //                    cmd.Parameters.AddWithValue("@FinalMercapesos", items.fecha2_mp == null ? DBNull.Value : items.fecha2_mp);
        //                    cmd.Parameters.AddWithValue("@DescuentoEspecial", items.desc_esp == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@FactorDescuento", items.fact_esp);
        //                    cmd.Parameters.AddWithValue("@ValorDescuentoFijo", items.valor_esp);
        //                    cmd.Parameters.AddWithValue("@InicioDescuento", items.fechdesci == null ? DBNull.Value : items.fechdesci);
        //                    cmd.Parameters.AddWithValue("@FinalDescuento", items.fechdescf == null ? DBNull.Value : items.fechdescf);
        //                    cmd.Parameters.AddWithValue("@DecodificarProducto", items.descod == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@DiaDecodificacion", items.descod_f == null ? DBNull.Value : items.descod_f);
        //                    cmd.Parameters.AddWithValue("@DiaUsuarioDecodificacion", items.fchdescusr == null ? DBNull.Value : items.fchdescusr);
        //                    cmd.Parameters.AddWithValue("@ValidarMaxVentasPorTercero", items.validmax == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@UnidadMaximaVentas", items.maxventa);
        //                    cmd.Parameters.AddWithValue("@NoIncluirDespacho", items.val_desp == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@FechaBloqueoPedido", items.f_bloqp == null ? DBNull.Value : items.f_bloqp);
        //                    cmd.Parameters.AddWithValue("@TipoControl", items.cont_devol);
        //                    cmd.Parameters.AddWithValue("@DiasSiguientesCompra", 0);
        //                    cmd.Parameters.AddWithValue("@PrecioVenta3", items.pvta3i);
        //                    cmd.Parameters.AddWithValue("@NoIncluirEnInventarioParaPedido", items.no_invped == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@AutorizarTrasladoProducto", items.aut_trasl == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@PrecioControlVingilancia", items.lp_cyvig == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@NoIncluirreporteChequeo", items.chequeo == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@NoCalcularCostoPromedio", items.costeo2 == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ProductoSobreStock", items.sobrestock == 1 ? true :false);
        //                    cmd.Parameters.AddWithValue("@EnviarAAsopanela", items.Asopanela ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@UnidadArticulo", 0);
        //                    cmd.Parameters.AddWithValue("@GrupoDescuento", string.IsNullOrWhiteSpace(items.grupodes)? DBNull.Value:items.grupodes);
        //                    cmd.Parameters.AddWithValue("@CodigoEan1", string.IsNullOrWhiteSpace(items.ean_1) ? DBNull.Value : items.ean_1);
        //                    cmd.Parameters.AddWithValue("@CodigoEan2", string.IsNullOrWhiteSpace(items.ean_2) ? DBNull.Value : items.ean_2);
        //                    cmd.Parameters.AddWithValue("@ProductoInAndOut", items.inandout == 1 ? true :false);
        //                    cmd.Parameters.AddWithValue("@DomiciliosCom", items.domi_com == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@PesoPOS", items.si_descto == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@OrdenadoInicioFactura", items.ord_prio == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ModificarConTolerancia", items.modq_reg ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@PorcentajeTolerancia", items.mod_toler);
        //                    cmd.Parameters.AddWithValue("@Stock", items.stockdomi);
        //                    cmd.Parameters.AddWithValue("@ExcluidoCOVID", items.ext_covid ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@unidcantfe", string.IsNullOrWhiteSpace(items.unidcantfe)?DBNull.Value:items.unidcantfe);
        //                    cmd.Parameters.AddWithValue("@tipobiend", string.IsNullOrWhiteSpace(items.tipobiend) ? DBNull.Value : items.tipobiend);
        //                    cmd.Parameters.AddWithValue("@DiasSinIVA", items.pdiasiva == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@DescuentoFNFP", items.es_dfnfp ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@IdVariedadFNFP", items.varifnfp);
        //                    cmd.Parameters.AddWithValue("@DescuentoNIIF", items.DESFINNIIF);
        //                    cmd.Parameters.AddWithValue("@NoUtilizar", false);
        //                    cmd.Parameters.AddWithValue("@FactorImpuestoConsumoRest", items.F_ICONSUMO == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@bod_asoc", items.bod_asoc);
        //                    cmd.Parameters.AddWithValue("@PrecioIva", items.pvtali);
        //                    cmd.Parameters.AddWithValue("@ValorIva", FCalcImp(items.pvta1i, items.iva, items.iconsumo, 1, 2, items.imp_salu, items.vr_imps, DateTime.Now, items.gen_impu, "I"));
        //                    cmd.Parameters.AddWithValue("@CostoAjusteNIIF", items.costoajus);
        //                    cmd.Parameters.AddWithValue("@DescuentoPorcentaje", items.descuento);
        //                    cmd.Parameters.AddWithValue("@RentabilidadSugerida", items.por_rentab);
        //                    cmd.Parameters.AddWithValue("@ConfirmarCambioPrecio", items.confirpre == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ProductoOfertado", items.ofertado ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@FechaPrimerMovimiento", items.fech1comp == null ? DBNull.Value : items.fech1comp);
        //                    cmd.Parameters.AddWithValue("@TipoImpuestoAlimentos", items.imp_salu);
        //                    cmd.Parameters.AddWithValue("@ValorTipoImpuestoAlimentos", items.vr_imps);
        //                    cmd.Parameters.AddWithValue("@GeneraImpuestoSaludable", items.gen_impu == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@CodigoReferencia", items.cod_ref);
        //                    cmd.Parameters.AddWithValue("@Referencia", items.refer);

        //                    // Ejecutar el comando de inserción
        //                    cmd.ExecuteNonQuery();



        //                    contador++; // Incrementar contador
        //                    Console.WriteLine($"Producto insertado #{contador}"); // Mostrar en consola
        //                }
        //                catch (Exception ex)
        //                {
        //                    Console.WriteLine($"⚠️ Error al insertar producto: {ex.Message}");


        //                    foreach (NpgsqlParameter p in cmd.Parameters)
        //                    {
        //                        if (p.Value is string s && s.Length > 50)
        //                        {
        //                            Console.WriteLine($"Posible error en parámetro: {p.ParameterName} (valor='{s}', longitud={s.Length})");
        //                        }
        //                    }

        //                    throw;


        //                    sw.Stop();

        //                    TimeSpan tiempoTran = sw.Elapsed;

        //                    cronometer.Stop();
        //                    Console.WriteLine("tiempo ejecucion"  + tiempoTran.ToString());
        //                    Console.WriteLine($"Guardado completado en: {cronometer.Elapsed.TotalMilliseconds} ms");


        //                    conn.Close();
        //                    //ex.InnerException
        //                }
        //            }

        //            // Confirmar la transacción
        //            tx.Commit();

        //        }
        //    }

        //    Console.WriteLine("Datos insertados correctamente.");

        //}

        public static void InsertarProductos(List<t_items> items_list, int batchSize = 300)
        {
            int contador = 0;
            var cronometer = Stopwatch.StartNew();
            Stopwatch sw = new Stopwatch();
            sw.Start();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            //using (var conn = new NpgsqlConnection("Host=190.107.27.195:9088;Username=postgres;Password=MC2022#;Database=mercacentro"))
            {
                conn.Open();

                while (contador < items_list.Count)
                {
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tx;

                        // 1. Ejecutar TRUNCATE con CASCADE antes de insertar
                        // cmd.CommandText = "TRUNCATE TABLE  \"Productos\" CASCADE;";
                        //cmd.ExecuteNonQuery();
                        //(SELECT DISTINCT ""id"" FROM ""SublineasProductos"" WHERE Codigo = @sublinea{contador} LIMIT 1),

                        // Verificar si el producto ya existe
                        



                        var commandText = new StringBuilder();
                        commandText.AppendLine(@"
                    INSERT INTO ""Productos"" (
                        ""Codigo"", ""IdTipoProducto"", ""FechaCreacion"", ""Nombre"", ""NombreCorto"", ""ReferenciaFabrica"",
                        ""PesoUnitario"", ""UnidadesPorCaja"", ""IdCategoria"", ""IdSubgruposProductos"", ""IdMarcasProductos"",
                        ""PesoDrenado"", ""Ean8"", ""Ean13"", ""Estado"", ""Unidad"", ""Talla"", ""IdLineasProductos"", ""IdSublineasProductos"",
                        ""NoAutorizar"", ""Inactivo"", ""EsKIT"", ""Iva"", ""Precio"", ""PrecioAnterior"", ""PrecioCambio"", ""Rentabilidad"",
                        ""BaseImpuestoConsumo"", ""IvaExento"", ""ImpuestoConsumo"", ""IdGrupoContable"", ""FactorImpuestoConsumo"",
                        ""IdRetFuente"", ""CostoAjuste"", ""Fruver"", ""BolsaAgro"", ""ModificaPrecio"", ""Fenaice"", ""AcumulaTira"",
                        ""SubsidioDesempleo"", ""ModificaCantidad"", ""BotFut"", ""ContabilizarGrabado"", ""NoAutorizaCompra"",
                        ""BalanzaToledo"", ""PrefijoEAN"", ""DescuentoAsohofrucol"", ""Motocicleta"", ""Pedido"", ""Codensa"", ""Ingreso"",
                        ""CheckeoPrecio"", ""IdEvento"", ""Tipo"", ""AdmiteDescuento"", ""DescuentoMaximo"", ""Decimales"", ""FechaCambio"",
                        ""CostoReposicion"", ""FenaicePorcentaje"", ""BotFutPorcentaje"", ""PrefijoEANPorcentaje"", ""CodensaPorcentaje"",
                        ""CodigoAlterno"", ""CCosto"", ""ExcluirDeListado"", ""PermitirSaldoEnRojo"", ""ExigirTarjetMercapesos"", ""Corrosivo"",
                        ""CAS"", ""CategoriaCAS"", ""PermitirVentaPorDebajoCosto"", ""DetalleAlElaborarDocumento"", ""UnicamenteManejarCantidad"",
                        ""SolicitarSerieAlFacturar"", ""TerceroAutomaticoPOS"", ""ProductoGenerico"", ""PesoBruto"", ""PesoNeto"", ""PesoTara"",
                        ""UnidadMinimaDeVenta"", ""Factor"", ""InicioMercapesos"", ""FinalMercapesos"", ""DescuentoEspecial"",
                        ""FactorDescuento"", ""ValorDescuentoFijo"", ""InicioDescuento"", ""FinalDescuento"", ""DecodificarProducto"",
                        ""DiaDecodificacion"", ""DiaUsuarioDecodificacion"", ""ValidarMaxVentasPorTercero"", ""UnidadMaximaVentas"", ""NoIncluirDespacho"", ""FechaBloqueoPedido"",
                        ""TipoControl"", ""DiasSiguientesCompra"", ""PrecioVenta3"", ""NoIncluirEnInventarioParaPedido"", ""AutorizarTrasladoProducto"",
                        ""PrecioControlVingilancia"", ""NoIncluirreporteChequeo"", ""NoCalcularCostoPromedio"", ""ProductoSobreStock"", ""EnviarAAsopanela"", ""UnidadArticulo"",
                        ""GrupoDescuento"", ""CodigoEan1"", ""CodigoEan2"", ""ProductoInAndOut"", ""DomiciliosCom"", ""PesoPOS"",
                        ""OrdenadoInicioFactura"", ""ModificarConTolerancia"", ""PorcentajeTolerancia"", ""Stock"", ""ExcluidoCOVID"", ""IdUnidadDIAN"",
                        ""IdTipoBienDIAN"", ""DiasSinIVA"", ""DescuentoFNFP"", ""IdVariedadFNFP"", ""DescuentoNIIF"", ""NoUtilizar"",
                        ""FactorImpuestoConsumoRest"", ""IdBodega"", ""IdUnidadProducto"", ""PrecioIva"", ""ValorIva"", ""CostoAjusteNIIF"",
                        ""DescuentoPorcentaje"", ""RentabilidadSugerida"", ""ConfirmarCambioPrecio"", ""ProductoOfertado"", ""FechaPrimerMovimiento"", ""TipoImpuestoAlimentos"",
                        ""ValorTipoImpuestoAlimentos"", ""GeneraImpuestoSaludable"", ""CodigoReferencia"", ""Referencia""
                    ) VALUES ");

                        var valuesList = new List<string>();
                        int currentBatchSize = 0;

                        while (contador < items_list.Count && currentBatchSize < batchSize)
                        {
                            var items = items_list[contador];
                            string shortnm = items.shortname?.Length > 50 ? items.shortname.Substring(0, 50) : items.shortname;

                            var values = $@"
                    (
                        @Codigo{contador}, @IdTipoProducto{contador}, @FechaCreacion{contador}, @Nombre{contador}, @NombreCorto{contador}, 
                        @ReferenciaFabrica{contador}, @PesoUnitario{contador}, @UnidadesPorCaja{contador}, @IdCategoria{contador},
                        (SELECT DISTINCT ""id"" FROM ""SubgruposProductos"" WHERE Codigo = @subgrupo{contador} and idsublineasproductos =(SELECT DISTINCT ""id"" FROM ""SublineasProductos"" WHERE Codigo = @sublinea{contador} and idlineasproductos = (SELECT DISTINCT ""id"" FROM ""LineasProductos"" WHERE Codigo = @linea{contador} LIMIT 1) LIMIT 1) LIMIT 1),
                        (SELECT DISTINCT ""id"" FROM ""MarcasProductos"" WHERE Codigo = @marca{contador} LIMIT 1),
                        @PesoDrenado{contador}, @Ean8{contador}, @Ean13{contador}, @Estado{contador}, @Unidad{contador}, @Talla{contador},
                        (SELECT DISTINCT ""id"" FROM ""LineasProductos"" WHERE Codigo = @linea{contador} LIMIT 1),                       
                        (SELECT DISTINCT ""id"" FROM ""SublineasProductos"" WHERE Codigo = @sublinea{contador} and idlineasproductos = (SELECT DISTINCT ""id"" FROM ""LineasProductos"" WHERE Codigo = @linea{contador} LIMIT 1) LIMIT 1),
                        @NoAutorizar{contador}, @Inactivo{contador}, @EsKIT{contador}, @Iva{contador}, @Precio{contador}, 
                        @PrecioAnterior{contador}, @PrecioCambio{contador}, @Rentabilidad{contador},
                        @BaseImpuestoConsumo{contador}, @IvaExento{contador}, @ImpuestoConsumo{contador}, @IdGrupoContable{contador}, 
                        @FactorImpuestoConsumo{contador}, @IdRetFuente{contador}, @CostoAjuste{contador}, @Fruver{contador}, 
                        @BolsaAgro{contador}, @ModificaPrecio{contador}, @Fenaice{contador}, @AcumulaTira{contador},
                        @SubsidioDesempleo{contador}, @ModificaCantidad{contador}, @BotFut{contador}, @ContabilizarGrabado{contador}, 
                        @NoAutorizaCompra{contador}, @BalanzaToledo{contador}, @PrefijoEAN{contador}, @DescuentoAsohofrucol{contador}, 
                        @Motocicleta{contador}, @Pedido{contador}, @Codensa{contador}, @Ingreso{contador},
                        @CheckeoPrecio{contador}, @IdEvento{contador}, @Tipo{contador}, @AdmiteDescuento{contador}, 
                        @DescuentoMaximo{contador}, @Decimales{contador}, @FechaCambio{contador},
                        @CostoReposicion{contador}, @FenaicePorcentaje{contador}, @BotFutPorcentaje{contador}, 
                        @PrefijoEANPorcentaje{contador}, @CodensaPorcentaje{contador},
                        @CodigoAlterno{contador}, @CCosto{contador}, @ExcluirDeListado{contador}, @PermitirSaldoEnRojo{contador}, 
                        @ExigirTarjetMercapesos{contador}, @Corrosivo{contador},
                        @CAS{contador}, @CategoriaCAS{contador}, @PermitirVentaPorDebajoCosto{contador}, 
                        @DetalleAlElaborarDocumento{contador}, @UnicamenteManejarCantidad{contador},
                        @SolicitarSerieAlFacturar{contador}, @TerceroAutomaticoPOS{contador}, @ProductoGenerico{contador}, 
                        @PesoBruto{contador}, @PesoNeto{contador}, @PesoTara{contador},
                        @UnidadMinimaDeVenta{contador}, @Factor{contador}, @InicioMercapesos{contador}, @FinalMercapesos{contador}, 
                        @DescuentoEspecial{contador},
                        @FactorDescuento{contador}, @ValorDescuentoFijo{contador}, @InicioDescuento{contador}, 
                        @FinalDescuento{contador}, @DecodificarProducto{contador},
                        @DiaDecodificacion{contador}, @DiaUsuarioDecodificacion{contador}, @ValidarMaxVentasPorTercero{contador}, 
                        @UnidadMaximaVentas{contador}, @NoIncluirDespacho{contador}, @FechaBloqueoPedido{contador},
                        @TipoControl{contador}, @DiasSiguientesCompra{contador}, @PrecioVenta3{contador}, 
                        @NoIncluirEnInventarioParaPedido{contador}, @AutorizarTrasladoProducto{contador},
                        @PrecioControlVingilancia{contador}, @NoIncluirreporteChequeo{contador}, @NoCalcularCostoPromedio{contador}, 
                        @ProductoSobreStock{contador}, @EnviarAAsopanela{contador}, @UnidadArticulo{contador},
                        @GrupoDescuento{contador}, @CodigoEan1{contador}, @CodigoEan2{contador}, @ProductoInAndOut{contador}, 
                        @DomiciliosCom{contador}, @PesoPOS{contador},
                        @OrdenadoInicioFactura{contador}, @ModificarConTolerancia{contador}, @PorcentajeTolerancia{contador}, 
                        @Stock{contador}, @ExcluidoCOVID{contador},
                        (SELECT DISTINCT ""Id"" FROM ""UnidadesDIAN"" WHERE ""Codigo"" = @unidcantfe{contador} LIMIT 1),
                        (SELECT DISTINCT ""Id"" FROM ""TipoBienDIAN"" WHERE ""Codigo"" = @tipobiend{contador} LIMIT 1),
                        @DiasSinIVA{contador}, @DescuentoFNFP{contador}, @IdVariedadFNFP{contador}, @DescuentoNIIF{contador}, 
                        @NoUtilizar{contador},
                        @FactorImpuestoConsumoRest{contador},
                        (SELECT DISTINCT id FROM bodegas WHERE Codigo = @bod_asoc{contador} LIMIT 1),
                        (SELECT DISTINCT id FROM unidaddemedida WHERE descripcion = @unidad{contador} LIMIT 1),
                        @PrecioIva{contador}, @ValorIva{contador}, @CostoAjusteNIIF{contador},
                        @DescuentoPorcentaje{contador}, @RentabilidadSugerida{contador}, @ConfirmarCambioPrecio{contador}, 
                        @ProductoOfertado{contador}, @FechaPrimerMovimiento{contador}, @TipoImpuestoAlimentos{contador},
                        @ValorTipoImpuestoAlimentos{contador}, @GeneraImpuestoSaludable{contador}, @CodigoReferencia{contador}, 
                        @Referencia{contador}
                    )";

                            valuesList.Add(values);
                            contador++;
                            currentBatchSize++;
                        }

                        // Combina todos los VALUES en una sola consulta
                        commandText.Append(string.Join(",", valuesList));

                        // Asigna el comando completo
                        cmd.CommandText = commandText.ToString();

                        // Agregar parámetros para el lote actual
                        for (int i = 0; i < currentBatchSize; i++)
                        {
                            var items = items_list[contador - currentBatchSize + i];
                            string shortnm = items.shortname?.Length > 50 ? items.shortname.Substring(0, 50) : items.shortname;


                            cmd.Parameters.AddWithValue($"@Codigo{contador - currentBatchSize + i}", items.Codigo);
                            //cmd.Parameters.AddWithValue($"@IdTipoProducto{contador - currentBatchSize + i}", int.Parse(items.tipo));
                            cmd.Parameters.AddWithValue(
                                $"@IdTipoProducto{contador - currentBatchSize + i}",
                                string.IsNullOrEmpty(items.tipo) ? (object)DBNull.Value : int.Parse(items.tipo)
                            );


                            cmd.Parameters.AddWithValue($"@FechaCreacion{contador - currentBatchSize + i}", items.fecha_cre);
                            cmd.Parameters.AddWithValue($"@Nombre{contador - currentBatchSize + i}", items.nombre);
                            cmd.Parameters.AddWithValue($"@NombreCorto{contador - currentBatchSize + i}", shortnm);
                            cmd.Parameters.AddWithValue($"@ReferenciaFabrica{contador - currentBatchSize + i}", items.refabrica);
                            cmd.Parameters.AddWithValue($"@PesoUnitario{contador - currentBatchSize + i}", items.peso_uni);
                            cmd.Parameters.AddWithValue($"@UnidadesPorCaja{contador - currentBatchSize + i}", items.undxcaja);
                            cmd.Parameters.AddWithValue($"@IdCategoria{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@subgrupo{contador - currentBatchSize + i}", items.subgrupo);
                            cmd.Parameters.AddWithValue($"@marca{contador - currentBatchSize + i}", items.marca);
                            cmd.Parameters.AddWithValue($"@PesoDrenado{contador - currentBatchSize + i}", items.pdrenado);
                            cmd.Parameters.AddWithValue($"@Ean8{contador - currentBatchSize + i}", items.cod_ean8);
                            cmd.Parameters.AddWithValue($"@Ean13{contador - currentBatchSize + i}", items.cod_bar);
                            cmd.Parameters.AddWithValue($"@Estado{contador - currentBatchSize + i}", items.bloqueado == 1 ? false : true);
                            cmd.Parameters.AddWithValue($"@Unidad{contador - currentBatchSize + i}", items.unidad);
                            cmd.Parameters.AddWithValue($"@Talla{contador - currentBatchSize + i}", items.talla);
                            cmd.Parameters.AddWithValue($"@linea{contador - currentBatchSize + i}", items.linea);
                            cmd.Parameters.AddWithValue($"@sublinea{contador - currentBatchSize + i}", items.sublinea);
                            cmd.Parameters.AddWithValue($"@NoAutorizar{contador - currentBatchSize + i}", items.no_compra);
                            cmd.Parameters.AddWithValue($"@Inactivo{contador - currentBatchSize + i}", items.bloqueado);
                            cmd.Parameters.AddWithValue($"@EsKIT{contador - currentBatchSize + i}", items.es_kitpro == 1 ? false : true);
                            cmd.Parameters.AddWithValue($"@Iva{contador - currentBatchSize + i}", items.iva);
                            cmd.Parameters.AddWithValue($"@Precio{contador - currentBatchSize + i}", items.pvtali);
                            cmd.Parameters.AddWithValue($"@PrecioAnterior{contador - currentBatchSize + i}", items.pvta_a1);
                            cmd.Parameters.AddWithValue($"@PrecioCambio{contador - currentBatchSize + i}", items.cambiopv_1);
                            cmd.Parameters.AddWithValue($"@Rentabilidad{contador - currentBatchSize + i}", CalcularRentabilidad(items.pvta1i, items.costo_rep, 3, items.iva, items.iconsumo, items.listap, items.imp_salu, items.vr_imps, DateTime.Now, items.gen_impu));
                            cmd.Parameters.AddWithValue($"@BaseImpuestoConsumo{contador - currentBatchSize + i}", items.iconsumo > 0 ? true : false);
                            cmd.Parameters.AddWithValue($"@IvaExento{contador - currentBatchSize + i}", items.excluido == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ImpuestoConsumo{contador - currentBatchSize + i}", items.listap);
                            cmd.Parameters.AddWithValue($"@IdGrupoContable{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@FactorImpuestoConsumo{contador - currentBatchSize + i}", items.F_ICONSUMO);
                            cmd.Parameters.AddWithValue($"@IdRetFuente{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@CostoAjuste{contador - currentBatchSize + i}", items.costoajus);
                            cmd.Parameters.AddWithValue($"@Fruver{contador - currentBatchSize + i}", items.es_fruver == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@BolsaAgro{contador - currentBatchSize + i}", items.bolsa == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ModificaPrecio{contador - currentBatchSize + i}", items.mod_ppos == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Fenaice{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.fenalce) ? true : false);
                            cmd.Parameters.AddWithValue($"@AcumulaTira{contador - currentBatchSize + i}", items.acu_tpos == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@SubsidioDesempleo{contador - currentBatchSize + i}", items.subsidio == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ModificaCantidad{contador - currentBatchSize + i}", items.mod_qpos == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@BotFut{contador - currentBatchSize + i}", items.es_bol == "1" ? true : false);
                            cmd.Parameters.AddWithValue($"@ContabilizarGrabado{contador - currentBatchSize + i}", items.contabgrav == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@NoAutorizaCompra{contador - currentBatchSize + i}", items.no_compra == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@BalanzaToledo{contador - currentBatchSize + i}", items.sitoledo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PrefijoEAN{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.pref_ean) ? true : false);
                            cmd.Parameters.AddWithValue($"@DescuentoAsohofrucol{contador - currentBatchSize + i}", items.es_bordado == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Motocicleta{contador - currentBatchSize + i}", items.es_moto == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Pedido{contador - currentBatchSize + i}", items.sipedido == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Codensa{contador - currentBatchSize + i}", items.escodensa == "S" ? true : false);
                            cmd.Parameters.AddWithValue($"@Ingreso{contador - currentBatchSize + i}", items.es_ingreso == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CheckeoPrecio{contador - currentBatchSize + i}", items.cheqpr == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@IdEvento{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            //cmd.Parameters.AddWithValue($"@Tipo{contador - currentBatchSize + i}", short.Parse(items.tipo));

                            cmd.Parameters.AddWithValue(
                              $"@Tipo{contador - currentBatchSize + i}",
                              string.IsNullOrEmpty(items.tipo) ? (object)DBNull.Value : int.Parse(items.tipo)
                          );

                            cmd.Parameters.AddWithValue($"@AdmiteDescuento{contador - currentBatchSize + i}", items.si_descto == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DescuentoMaximo{contador - currentBatchSize + i}", items.descmax);
                            cmd.Parameters.AddWithValue($"@Decimales{contador - currentBatchSize + i}", items.deci_cant);
                            cmd.Parameters.AddWithValue($"@FechaCambio{contador - currentBatchSize + i}", items.fech_cp1);
                            cmd.Parameters.AddWithValue($"@CostoReposicion{contador - currentBatchSize + i}", items.costo_rep);
                            cmd.Parameters.AddWithValue($"@FenaicePorcentaje{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@BotFutPorcentaje{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@PrefijoEANPorcentaje{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.pref_ean) ? 0 : int.Parse(items.pref_ean));
                            cmd.Parameters.AddWithValue($"@CodensaPorcentaje{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@CodigoAlterno{contador - currentBatchSize + i}", items.cod_alt);
                            cmd.Parameters.AddWithValue($"@CCosto{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.CCosto) ? DBNull.Value : int.Parse(items.CCosto));
                            cmd.Parameters.AddWithValue($"@ExcluirDeListado{contador - currentBatchSize + i}", items.elegido == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PermitirSaldoEnRojo{contador - currentBatchSize + i}", items.sdo_rojo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ExigirTarjetMercapesos{contador - currentBatchSize + i}", items.pidempeso == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Corrosivo{contador - currentBatchSize + i}", items.corrosivo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CAS{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.n_cas) ? DBNull.Value : items.n_cas);
                            cmd.Parameters.AddWithValue($"@CategoriaCAS{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.cat_cas) ? DBNull.Value : items.cat_cas);
                            cmd.Parameters.AddWithValue($"@PermitirVentaPorDebajoCosto{contador - currentBatchSize + i}", items.vta_costo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DetalleAlElaborarDocumento{contador - currentBatchSize + i}", items.si_detdoc == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@UnicamenteManejarCantidad{contador - currentBatchSize + i}", items.solocant == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@SolicitarSerieAlFacturar{contador - currentBatchSize + i}", items.si_serie == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@TerceroAutomaticoPOS{contador - currentBatchSize + i}", items.terceAutom == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ProductoGenerico{contador - currentBatchSize + i}", items.generico == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PesoBruto{contador - currentBatchSize + i}", items.pesocajb);
                            cmd.Parameters.AddWithValue($"@PesoNeto{contador - currentBatchSize + i}", items.pesocajn);
                            cmd.Parameters.AddWithValue($"@PesoTara{contador - currentBatchSize + i}", items.peso_car);
                            cmd.Parameters.AddWithValue($"@UnidadMinimaDeVenta{contador - currentBatchSize + i}", items.unidmin);
                            cmd.Parameters.AddWithValue($"@Factor{contador - currentBatchSize + i}", items.puntaje);
                            cmd.Parameters.AddWithValue($"@InicioMercapesos{contador - currentBatchSize + i}", items.fecha1_mp == null ? DBNull.Value : items.fecha1_mp);
                            cmd.Parameters.AddWithValue($"@FinalMercapesos{contador - currentBatchSize + i}", items.fecha2_mp == null ? DBNull.Value : items.fecha2_mp);
                            cmd.Parameters.AddWithValue($"@DescuentoEspecial{contador - currentBatchSize + i}", items.desc_esp == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@FactorDescuento{contador - currentBatchSize + i}", items.fact_esp);
                            cmd.Parameters.AddWithValue($"@ValorDescuentoFijo{contador - currentBatchSize + i}", items.valor_esp);
                            cmd.Parameters.AddWithValue($"@InicioDescuento{contador - currentBatchSize + i}", items.fechdesci == null ? DBNull.Value : items.fechdesci);
                            cmd.Parameters.AddWithValue($"@FinalDescuento{contador - currentBatchSize + i}", items.fechdescf == null ? DBNull.Value : items.fechdescf);
                            cmd.Parameters.AddWithValue($"@DecodificarProducto{contador - currentBatchSize + i}", items.descod == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DiaDecodificacion{contador - currentBatchSize + i}", items.descod_f == null ? DBNull.Value : items.descod_f);
                            cmd.Parameters.AddWithValue($"@DiaUsuarioDecodificacion{contador - currentBatchSize + i}", items.fchdescusr == null ? DBNull.Value : items.fchdescusr);
                            cmd.Parameters.AddWithValue($"@ValidarMaxVentasPorTercero{contador - currentBatchSize + i}", items.validmax == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@UnidadMaximaVentas{contador - currentBatchSize + i}", items.maxventa);
                            cmd.Parameters.AddWithValue($"@NoIncluirDespacho{contador - currentBatchSize + i}", items.val_desp == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@FechaBloqueoPedido{contador - currentBatchSize + i}", items.f_bloqp == null ? DBNull.Value : items.f_bloqp);
                            cmd.Parameters.AddWithValue($"@TipoControl{contador - currentBatchSize + i}", items.cont_devol);
                            cmd.Parameters.AddWithValue($"@DiasSiguientesCompra{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@PrecioVenta3{contador - currentBatchSize + i}", items.pvta3i);
                            cmd.Parameters.AddWithValue($"@NoIncluirEnInventarioParaPedido{contador - currentBatchSize + i}", items.no_invped == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@AutorizarTrasladoProducto{contador - currentBatchSize + i}", items.aut_trasl == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PrecioControlVingilancia{contador - currentBatchSize + i}", items.lp_cyvig == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@NoIncluirreporteChequeo{contador - currentBatchSize + i}", items.chequeo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@NoCalcularCostoPromedio{contador - currentBatchSize + i}", items.costeo2 == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ProductoSobreStock{contador - currentBatchSize + i}", items.sobrestock == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EnviarAAsopanela{contador - currentBatchSize + i}", items.Asopanela == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@UnidadArticulo{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@GrupoDescuento{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.grupodes) ? DBNull.Value : items.grupodes);
                            cmd.Parameters.AddWithValue($"@CodigoEan1{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.ean_1) ? DBNull.Value : items.ean_1);
                            cmd.Parameters.AddWithValue($"@CodigoEan2{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.ean_2) ? DBNull.Value : items.ean_2);
                            cmd.Parameters.AddWithValue($"@ProductoInAndOut{contador - currentBatchSize + i}", items.inandout == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DomiciliosCom{contador - currentBatchSize + i}", items.domi_com == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PesoPOS{contador - currentBatchSize + i}", items.si_descto == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@OrdenadoInicioFactura{contador - currentBatchSize + i}", items.ord_prio == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ModificarConTolerancia{contador - currentBatchSize + i}", items.modq_reg == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PorcentajeTolerancia{contador - currentBatchSize + i}", items.mod_toler);
                            cmd.Parameters.AddWithValue($"@Stock{contador - currentBatchSize + i}", items.stockdomi);
                            cmd.Parameters.AddWithValue($"@ExcluidoCOVID{contador - currentBatchSize + i}", items.ext_covid == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@unidcantfe{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.unidcantfe) ? DBNull.Value : items.unidcantfe);
                            cmd.Parameters.AddWithValue($"@tipobiend{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.tipobiend) ? DBNull.Value : items.tipobiend);
                            cmd.Parameters.AddWithValue($"@DiasSinIVA{contador - currentBatchSize + i}", items.pdiasiva == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DescuentoFNFP{contador - currentBatchSize + i}", items.es_dfnfp == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@IdVariedadFNFP{contador - currentBatchSize + i}", items.varifnfp);
                            cmd.Parameters.AddWithValue($"@DescuentoNIIF{contador - currentBatchSize + i}", items.DESFINNIIF);
                            cmd.Parameters.AddWithValue($"@NoUtilizar{contador - currentBatchSize + i}", false);
                            cmd.Parameters.AddWithValue($"@FactorImpuestoConsumoRest{contador - currentBatchSize + i}", items.F_ICONSUMO == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@bod_asoc{contador - currentBatchSize + i}", items.bod_asoc);
                            cmd.Parameters.AddWithValue($"@PrecioIva{contador - currentBatchSize + i}", items.pvtali);
                            cmd.Parameters.AddWithValue($"@ValorIva{contador - currentBatchSize + i}", FCalcImp(items.pvta1i, items.iva, items.iconsumo, 1, 2, items.imp_salu, items.vr_imps, DateTime.Now, items.gen_impu, "I"));
                            cmd.Parameters.AddWithValue($"@CostoAjusteNIIF{contador - currentBatchSize + i}", items.costoajus);
                            cmd.Parameters.AddWithValue($"@DescuentoPorcentaje{contador - currentBatchSize + i}", items.descuento);
                            cmd.Parameters.AddWithValue($"@RentabilidadSugerida{contador - currentBatchSize + i}", items.por_rentab);
                            cmd.Parameters.AddWithValue($"@ConfirmarCambioPrecio{contador - currentBatchSize + i}", items.confirpre == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ProductoOfertado{contador - currentBatchSize + i}", items.ofertado == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@FechaPrimerMovimiento{contador - currentBatchSize + i}", items.fech1comp == null ? DBNull.Value : items.fech1comp);
                            cmd.Parameters.AddWithValue($"@TipoImpuestoAlimentos{contador - currentBatchSize + i}", items.imp_salu);
                            cmd.Parameters.AddWithValue($"@ValorTipoImpuestoAlimentos{contador - currentBatchSize + i}", items.vr_imps);
                            cmd.Parameters.AddWithValue($"@GeneraImpuestoSaludable{contador - currentBatchSize + i}", items.gen_impu == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CodigoReferencia{contador - currentBatchSize + i}", items.cod_ref);
                            cmd.Parameters.AddWithValue($"@Referencia{contador - currentBatchSize + i}", items.refer);
                        }

                        commandText.Append(string.Join(",", valuesList));
                        commandText.AppendLine(@" ON CONFLICT (""Codigo"") DO NOTHING;");


                        cmd.ExecuteNonQuery();
                        tx.Commit();
                        
                    }
                }

                conn.Close();
            }
            cronometer.Stop();

            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
            Console.ReadKey();

        }

        public static async Task insertarTerceros(List<t_terceros> terceros_list,List<t_ciudad> ciudad_list ,int batchSize = 700)
        {
            int contador = 0;
            var cronometer = Stopwatch.StartNew();
            Stopwatch sw = new Stopwatch();
            sw.Start();


            Console.WriteLine("Ingreso a insercion");

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                await conn.OpenAsync();

                var tiposdedocumento = new Dictionary<string, int>();
                var municipio = new Dictionary<(string codigodepartamento, string codigo),  int>();
                var departamento = new Dictionary<string, int>();
                var respFisc = new Dictionary<string, int>();
                var respTrib = new Dictionary<string, int>();



                using (var cmd = new NpgsqlCommand(@"SELECT codigo,id  FROM public.responsabilidadesfiscales", conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        string codigo = reader.GetString(0);
                        int id = reader.GetInt32(1);

                        respFisc[codigo] = id;
                    }
                }

                using (var cmd = new NpgsqlCommand(@"SELECT codigo,id  FROM public.responsabilidadestributarias", conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        string codigo = reader.GetString(0);
                        int id = reader.GetInt32(1);

                        respTrib[codigo] = id;
                    }
                }


                using (var cmd = new NpgsqlCommand(@"SELECT Id, Codigo FROM public.tiposdedocumento", conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        int id = reader.GetInt32(0);
                        string codigo = reader.GetString(1);

                        tiposdedocumento[codigo] = id;
                    }
                }

                using (var cmd = new NpgsqlCommand(@"SELECT codigodepartamento,codigo,id FROM public.municipios", conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        string codigodepartamento = reader.GetString(0);
                        string codigo = reader.GetString(1);
                        int id = reader.GetInt32(2);
                        

                        municipio[(codigodepartamento, codigo)] = id;
                    }
                }

                using (var cmd = new NpgsqlCommand(@"SELECT codigo,id FROM public.departamentos", conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {

                        string codigo = reader.GetString(0);
                        int id = reader.GetInt32(1);
                        

                        departamento[codigo] = id;
                    }
                }


                while (contador < terceros_list.Count)
                {
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tx;

       
                        var commandText = new StringBuilder();

                        commandText.AppendLine(@"
                        INSERT INTO ""Terceros"" (
                        ""TipoPersona"", ""IdTipoIdentificacion"", ""Identificacion"", ""Nombre1"", ""Nombre2"", 
                        ""Apellido1"", ""Apellido2"", ""Genero"", ""FechaNacimiento"", ""RazonSocial"", 
                        ""NombreComercial"", ""Direccion"", ""Email"", ""Email2"", ""IdDepartamento"", 
                        ""IdMunicipio"", ""Telefono1"", ""Telefono2"", ""Estado"", ""EsCliente"", 
                        ""EsEmpleado"", ""EsPasante"", ""EsProveedor"", ""FechaCreacion"", ""FechaActualizacion"", 
                        ""DiasCredito"", ""EsProveedorFruver"", ""EsProveedorBolsaAgropecuaria"", ""EsProveedorCampesinoDirecto"", 
                        ""EsProveedorRestaurante"", ""EsProveedorPanaderia"", ""EsOtroTipo"", ""EsGasto"", 
                        ""CotizaEPS"", ""CotizaFondoPension"", ""CotizaARP"", ""TarifaARP"", ""RegimenSimplificado"", 
                        ""NoPracticarRetFuente"", ""NoPracticarRetIVA"", ""Autorretenedor"", ""EsRetenedorFuente"", 
                        ""DescontarAsohofrucol"", ""AsumirImpuestos"", ""RetenerFenalce"", ""AsumirFenalce"", 
                        ""BolsaAgropecuaria"", ""RegimenComun"", ""RetenerSiempre"", ""GranContribuyente"", 
                        ""AutorretenedorIVA"", ""IdCxPagar"", ""DeclaranteRenta"", ""DescuentoNIIF"", 
                        ""DescontarFNFP"", ""ManejaIVAProductoBonificado"", ""ReteIVALey560_2020"", ""RegimenSimpleTributacion"", 
                        ""RetencionZOMAC"", ""TipoDescuentoFinanciero"", ""Porcentaje1"", ""Porcentaje2"", 
                        ""Porcentaje3"", ""Porcentaje4"", ""Porcentaje5"", ""IdRUT"", ""IdICATerceroCiudad"", 
                        ""ClienteExcentoIVA"", ""EstadoRUT"", ""FechaRut"", ""IdCxCobrar"", ""DigitoVerificacion"", 
                        ""IdEmpleado"", ""BaseDecreciente"", ""IdRegimenContribuyente"", ""IdResponsabilidadesFiscales"", 
                        ""IdResponsabilidadesTributarias"", ""IdUbicacionDANE"", ""CodigoPostal"", ""BloqueoPago"", 
                        ""ObservacionBloqueo"", ""FrecuenciaServicio"", ""PromesaServicio"", ""DiasInvSeguridad""
                        )VALUES");

                        var valuesList = new List<string>();
                        int currentBatchSize = 0;

                        while (contador < terceros_list.Count && currentBatchSize < batchSize)
                        {
                            var items = terceros_list[contador];

                            
                            var values = $@"
                    (
                         @TipoPersona{contador},
                         @IdTipoIdentificacion{contador}, 
                         @Identificacion{contador}, @Nombre1{contador}, @Nombre2{contador}, @Apellido1{contador}, 
                         @Apellido2{contador}, @Genero{contador}, @FechaNacimiento{contador}, @RazonSocial{contador}, @NombreComercial{contador}, @Direccion{contador}, 
                         @Email{contador}, @Email2{contador}, 
                         @IdDepartamento{contador}, 
                         @IdMunicipio{contador}, 
                         @Telefono1{contador}, @Telefono2{contador}, @Estado{contador}, 
                         @EsCliente{contador}, @EsEmpleado{contador}, @EsPasante{contador}, @EsProveedor{contador}, 
                         @FechaCreacion{contador}, @FechaActualizacion{contador}, @DiasCredito{contador}, 
                         @EsProveedorFruver{contador}, @EsProveedorBolsaAgropecuaria{contador}, @EsProveedorCampesinoDirecto{contador}, 
                         @EsProveedorRestaurante{contador}, @EsProveedorPanaderia{contador}, @EsOtroTipo{contador}, @EsGasto{contador}, 
                         @CotizaEPS{contador}, @CotizaFondoPension{contador}, @CotizaARP{contador}, @TarifaARP{contador}, 
                         @RegimenSimplificado{contador}, @NoPracticarRetFuente{contador}, @NoPracticarRetIVA{contador}, 
                         @Autorretenedor{contador}, @EsRetenedorFuente{contador}, @DescontarAsohofrucol{contador}, 
                         @AsumirImpuestos{contador}, @RetenerFenalce{contador}, @AsumirFenalce{contador}, 
                         @BolsaAgropecuaria{contador}, @RegimenComun{contador}, @RetenerSiempre{contador}, 
                         @GranContribuyente{contador}, @AutorretenedorIVA{contador}, @IdCxPagar{contador}, 
                         @DeclaranteRenta{contador}, @DescuentoNIIF{contador}, @DescontarFNFP{contador}, 
                         @ManejaIVAProductoBonificado{contador}, @ReteIVALey560_2020{contador}, 
                         @RegimenSimpleTributacion{contador}, @RetencionZOMAC{contador}, @TipoDescuentoFinanciero{contador}, 
                         @Porcentaje1{contador}, @Porcentaje2{contador}, @Porcentaje3{contador}, @Porcentaje4{contador}, @Porcentaje5{contador}, 
                         @IdRUT{contador}, @IdICATerceroCiudad{contador}, @ClienteExcentoIVA{contador}, 
                         @EstadoRUT{contador}, @FechaRut{contador}, @IdCxCobrar{contador}, @DigitoVerificacion{contador}, 
                         @IdEmpleado{contador}, @BaseDecreciente{contador}, @IdRegimenContribuyente{contador}, 
                         @IdResponsabilidadesFiscales{contador}, @IdResponsabilidadesTributarias{contador}, 
                         @IdUbicacionDANE{contador}, @CodigoPostal{contador}, @BloqueoPago{contador}, 
                         @ObservacionBloqueo{contador}, @FrecuenciaServicio{contador}, @PromesaServicio{contador}, 
                         @DiasInvSeguridad{contador}
                         )";

                            valuesList.Add(values);
                            contador++;
                            currentBatchSize++;
                        }

                        // Combina todos los VALUES en una sola consulta
                        commandText.Append(string.Join(",", valuesList));

                        // Asigna el comando completo
                        cmd.CommandText = commandText.ToString();

                        // Agregar parámetros para el lote actual
                        for (int i = 0; i < currentBatchSize; i++)
                        {

                            string xDepartamento = "";
                            string xMunicipio    = "";
                            string parte1 = "";
                            string parte2 = "";
                            int    xPersona = 0;

                            var anexos = terceros_list[contador - currentBatchSize + i];

                            var ubicacion = ciudad_list.FirstOrDefault(c => c.Dane == anexos.IdUbicacionDANE);


                            if (ubicacion != null)
                            {
                                parte1 = ubicacion.Dane.Substring(0, 2);
                                parte2 = ubicacion.Dane.Substring(2, 3);
                                xDepartamento = ubicacion.Departamento;
                                xMunicipio    = ubicacion.Municipio;
                            }

                            if (anexos.TipoPersona == "Natural")
                            {
                                xPersona = 1;
                            }else
                            {
                                xPersona = 2;
                            }

                            cmd.Parameters.AddWithValue($"@TipoPersona{contador - currentBatchSize + i}", xPersona);

                            cmd.Parameters.AddWithValue($"@IdTipoIdentificacion{contador - currentBatchSize + i}",
                                    tiposdedocumento.ContainsKey(anexos.IdTipoIdentificacion.ToString())
                                      ? tiposdedocumento[anexos.IdTipoIdentificacion.ToString()]
                                      : (object)DBNull.Value);

                            //cmd.Parameters.AddWithValue($"@IdTipoIdentificacion{contador - currentBatchSize + i}", anexos.IdTipoIdentificacion.ToString());
                            cmd.Parameters.AddWithValue($"@Identificacion{contador - currentBatchSize + i}", anexos.Identificacion);
                            cmd.Parameters.AddWithValue($"@Nombre1{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Nombre1) ? anexos.Nombre1 : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Nombre2{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Nombre2) ? anexos.Nombre2 : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Apellido1{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Apellido1) ? anexos.Apellido1 : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Apellido2{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Apellido2) ? anexos.Apellido2 : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Genero{contador - currentBatchSize + i}", anexos.Genero == "M" ? 1 : 2);
                            cmd.Parameters.AddWithValue($"@FechaNacimiento{contador - currentBatchSize + i}",  anexos.FechaNacimiento);
                            cmd.Parameters.AddWithValue($"@RazonSocial{contador - currentBatchSize + i}", anexos.RazonSocial);
                            cmd.Parameters.AddWithValue($"@NombreComercial{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.NombreComercial) ? anexos.NombreComercial : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Direccion{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Direccion) ? anexos.Direccion : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Email{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Email) ? anexos.Email : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Email2{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Email2) ? anexos.Email2 : (object)DBNull.Value);

                            cmd.Parameters.AddWithValue($"@IdDepartamento{contador - currentBatchSize + i}",
                                   departamento.ContainsKey(parte1)
                                     ? departamento[parte1]
                                     : (object)DBNull.Value);


                            cmd.Parameters.Add($"@IdMunicipio{contador - currentBatchSize + i}", NpgsqlTypes.NpgsqlDbType.Integer).Value =
                              municipio.TryGetValue((parte1, parte2), out int IdMunicipio)
                                  ? IdMunicipio
                                  : (object)DBNull.Value;

                            //cmd.Parameters.AddWithValue($"@IdDepartamento{contador - currentBatchSize + i}", string.IsNullOrEmpty(xDepartamento) ? (object)DBNull.Value : $"(SELECT DISTINCT id FROM departamentos WHERE departamentos.nombre = '{xDepartamento}' LIMIT 1)");
                            //cmd.Parameters.AddWithValue($"@IdMunicipio{contador - currentBatchSize + i}", string.IsNullOrEmpty(xMunicipio) ? (object)DBNull.Value : $"(SELECT DISTINCT id FROM municipios WHERE municipios.nombre = '{xMunicipio}' LIMIT 1)");

                            cmd.Parameters.AddWithValue($"@Telefono1{contador - currentBatchSize + i}", anexos.Telefono1);
                            cmd.Parameters.AddWithValue($"@Telefono2{contador - currentBatchSize + i}", anexos.Telefono2);
                            cmd.Parameters.AddWithValue($"@Estado{contador - currentBatchSize + i}", anexos.Estado == 1 ? false : true);
                            cmd.Parameters.AddWithValue($"@EsCliente{contador - currentBatchSize + i}", anexos.EsCliente == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsEmpleado{contador - currentBatchSize + i}", anexos.EsEmpleado == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsPasante{contador - currentBatchSize + i}", anexos.EsPasante == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsProveedor{contador - currentBatchSize + i}", anexos.EsProveedor == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@FechaCreacion{contador - currentBatchSize + i}", anexos.FechaCreacion.HasValue ? anexos.FechaCreacion: DateTime.Now);
                            cmd.Parameters.AddWithValue($"@FechaActualizacion{contador - currentBatchSize + i}", anexos.FechaActualizacion.HasValue ? anexos.FechaActualizacion : DateTime.Now);
                            cmd.Parameters.AddWithValue($"@DiasCredito{contador - currentBatchSize + i}", anexos.DiasCredito);
                            cmd.Parameters.AddWithValue($"@EsProveedorFruver{contador - currentBatchSize + i}", anexos.EsProveedorFruver == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsProveedorBolsaAgropecuaria{contador - currentBatchSize + i}", anexos.EsProveedorBolsaAgropecuaria == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsProveedorCampesinoDirecto{contador - currentBatchSize + i}", anexos.EsProveedorCampesinoDirecto == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsProveedorRestaurante{contador - currentBatchSize + i}", anexos.EsProveedorRestaurante == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsProveedorPanaderia{contador - currentBatchSize + i}", anexos.EsProveedorPanaderia == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsOtroTipo{contador - currentBatchSize + i}", anexos.EsOtroTipo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsGasto{contador - currentBatchSize + i}", anexos.EsGasto == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CotizaEPS{contador - currentBatchSize + i}", anexos.CotizaEPS == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CotizaFondoPension{contador - currentBatchSize + i}", anexos.CotizaFondoPension == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CotizaARP{contador - currentBatchSize + i}", anexos.CotizaARP == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@TarifaARP{contador - currentBatchSize + i}", anexos.TarifaARP);
                            cmd.Parameters.AddWithValue($"@RegimenSimplificado{contador - currentBatchSize + i}", anexos.RegimenSimplificado == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@NoPracticarRetFuente{contador - currentBatchSize + i}", anexos.NoPracticarRetFuente == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@NoPracticarRetIVA{contador - currentBatchSize + i}", anexos.NoPracticarRetIVA == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Autorretenedor{contador - currentBatchSize + i}", anexos.Autorretenedor == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsRetenedorFuente{contador - currentBatchSize + i}", anexos.EsRetenedorFuente == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DescontarAsohofrucol{contador - currentBatchSize + i}", anexos.DescontarAsohofrucol == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@AsumirImpuestos{contador - currentBatchSize + i}", anexos.AsumirImpuestos == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RetenerFenalce{contador - currentBatchSize + i}", anexos.RetenerFenalce == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@AsumirFenalce{contador - currentBatchSize + i}", anexos.AsumirFenalce == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@BolsaAgropecuaria{contador - currentBatchSize + i}", anexos.BolsaAgropecuaria == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RegimenComun{contador - currentBatchSize + i}", anexos.RegimenComun == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RetenerSiempre{contador - currentBatchSize + i}", anexos.RetenerSiempre == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@GranContribuyente{contador - currentBatchSize + i}", anexos.GranContribuyente == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@AutorretenedorIVA{contador - currentBatchSize + i}", anexos.AutorretenedorIVA == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@IdCxPagar{contador - currentBatchSize + i}", anexos.IdCxPagar == 0 ? 0 : anexos.IdCxPagar);
                            cmd.Parameters.AddWithValue($"@DeclaranteRenta{contador - currentBatchSize + i}", anexos.DeclaranteRenta == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DescuentoNIIF{contador - currentBatchSize + i}", anexos.DescuentoNIIF == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DescontarFNFP{contador - currentBatchSize + i}", anexos.DescontarFNFP == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ManejaIVAProductoBonificado{contador - currentBatchSize + i}", anexos.ManejaIVAProductoBonificado == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ReteIVALey560_2020{contador - currentBatchSize + i}", anexos.ReteIVALey560_2020 == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RegimenSimpleTributacion{contador - currentBatchSize + i}", anexos.RegimenSimpleTributacion == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RetencionZOMAC{contador - currentBatchSize + i}", false);
                            cmd.Parameters.AddWithValue($"@TipoDescuentoFinanciero{contador - currentBatchSize + i}", anexos.TipoDescuentoFinanciero);
                            cmd.Parameters.AddWithValue($"@Porcentaje1{contador - currentBatchSize + i}", anexos.Porcentaje1);
                            cmd.Parameters.AddWithValue($"@Porcentaje2{contador - currentBatchSize + i}", anexos.Porcentaje2);
                            cmd.Parameters.AddWithValue($"@Porcentaje3{contador - currentBatchSize + i}", anexos.Porcentaje3);
                            cmd.Parameters.AddWithValue($"@Porcentaje4{contador - currentBatchSize + i}", anexos.Porcentaje4);
                            cmd.Parameters.AddWithValue($"@Porcentaje5{contador - currentBatchSize + i}", anexos.Porcentaje5);
                            cmd.Parameters.AddWithValue($"@IdRUT{contador - currentBatchSize + i}", anexos.IdRUT);
                            cmd.Parameters.AddWithValue($"@IdICATerceroCiudad{contador - currentBatchSize + i}", anexos.IdICATerceroCiudad);
                            cmd.Parameters.AddWithValue($"@ClienteExcentoIVA{contador - currentBatchSize + i}", false);
                            cmd.Parameters.AddWithValue($"@EstadoRUT{contador - currentBatchSize + i}", (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@FechaRut{contador - currentBatchSize + i}", anexos.FechaRut.HasValue ? anexos.FechaRut : DateTime.Now);
                            cmd.Parameters.AddWithValue($"@IdCxCobrar{contador - currentBatchSize + i}", anexos.IdCxCobrar == 0 ? 0 : anexos.IdCxCobrar);
                            cmd.Parameters.AddWithValue($"@DigitoVerificacion{contador - currentBatchSize + i}", string.IsNullOrEmpty(anexos.DigitoVerificacion) ? (object)DBNull.Value : anexos.DigitoVerificacion);
                            cmd.Parameters.AddWithValue($"@IdEmpleado{contador - currentBatchSize + i}", anexos.IdEmpleado == 0 ? 0 : $"(SELECT DISTINCT \"Id\" FROM \"Empleados\" WHERE \"NumerodeDocumento\" = '{anexos.IdTipoIdentificacion}' LIMIT 1)");
                            cmd.Parameters.AddWithValue($"@BaseDecreciente{contador - currentBatchSize + i}", anexos.BaseDecreciente == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RentabilidadSugerida{contador - currentBatchSize + i}", "0");
                            cmd.Parameters.AddWithValue($"@IdRegimenContribuyente{contador - currentBatchSize + i}", 0);

                            // Manejo de subconsultas
                            //int? idResponsabilidadesFiscales = null;
                            //if (!string.IsNullOrEmpty(anexos.IdResponsabilidadesFiscales))
                            //{
                            //    using (var cmdSubquery = new NpgsqlCommand("SELECT DISTINCT id FROM responsabilidadesfiscales WHERE codigo = @codigo LIMIT 1", conn))
                            //    {
                            //        cmdSubquery.Parameters.AddWithValue("@codigo", anexos.IdResponsabilidadesFiscales);
                            //        var result = cmdSubquery.ExecuteScalar();
                            //        if (result != null)
                            //        {
                            //            idResponsabilidadesFiscales = Convert.ToInt32(result);
                            //        }
                            //    }
                            //}

                            //cmd.Parameters.AddWithValue($"@IdResponsabilidadesFiscales{contador - currentBatchSize + i}", idResponsabilidadesFiscales.HasValue ? (object)idResponsabilidadesFiscales.Value : DBNull.Value);

                            cmd.Parameters.AddWithValue($"@IdResponsabilidadesFiscales{contador - currentBatchSize + i}",
                             respFisc.ContainsKey(anexos.IdResponsabilidadesFiscales)
                               ? respFisc[anexos.IdResponsabilidadesFiscales]
                               : (object)DBNull.Value);


                            //int? idResponsabilidadesTributarias = null;
                            //if (!string.IsNullOrEmpty(anexos.IdResponsabilidadesTributarias))
                            //{
                            //    using (var cmdSubquery = new NpgsqlCommand("SELECT DISTINCT id FROM responsabilidadestributarias WHERE codigo = @codigo LIMIT 1", conn))
                            //    {
                            //        cmdSubquery.Parameters.AddWithValue("@codigo", anexos.IdResponsabilidadesTributarias);
                            //        var result = cmdSubquery.ExecuteScalar();
                            //        if (result != null)
                            //        {
                            //            idResponsabilidadesTributarias = Convert.ToInt32(result);
                            //        }
                            //    }
                            //}

                            //cmd.Parameters.AddWithValue($"@IdResponsabilidadesTributarias{contador - currentBatchSize + i}", idResponsabilidadesTributarias.HasValue ? (object)idResponsabilidadesTributarias.Value : DBNull.Value);


                            cmd.Parameters.AddWithValue($"@IdResponsabilidadesTributarias{contador - currentBatchSize + i}",
                             respTrib.ContainsKey(anexos.IdResponsabilidadesTributarias)
                               ? respTrib[anexos.IdResponsabilidadesTributarias]
                               : (object)DBNull.Value);


                            cmd.Parameters.AddWithValue($"@idubicaciondane{contador - currentBatchSize + i}", string.IsNullOrEmpty(anexos.IdUbicacionDANE) ? 0 : Convert.ToInt32(anexos.IdUbicacionDANE));
                            cmd.Parameters.AddWithValue($"@CodigoPostal{contador - currentBatchSize + i}", string.IsNullOrEmpty(anexos.CodigoPostal) ? (object)DBNull.Value : anexos.CodigoPostal);
                            cmd.Parameters.AddWithValue($"@BloqueoPago{contador - currentBatchSize + i}", false);
                            cmd.Parameters.AddWithValue($"@ObservacionBloqueo{contador - currentBatchSize + i}", (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@FrecuenciaServicio{contador - currentBatchSize + i}", (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@PromesaServicio{contador - currentBatchSize + i}", (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@DiasInvSeguridad{contador - currentBatchSize + i}", (object)DBNull.Value);


                        }

                        if (contador % 700 == 0)
                            Console.WriteLine("ejecuto execute");

                        //File.WriteAllText(@"C:\tmp_archivos\sql_debug.txt", cmd.CommandText);

                        //string sqlConValores = cmd.CommandText;

                        //foreach (NpgsqlParameter p in cmd.Parameters)
                        //{
                        //    string valor = p.Value == null ? "NULL" :
                        //                   p.Value is string ? $"'{p.Value}'" :
                        //                   p.Value is DateTime ? $"'{((DateTime)p.Value):yyyy-MM-dd HH:mm:ss}'" :
                        //                   p.Value.ToString();

                        //    sqlConValores = sqlConValores.Replace(p.ParameterName, valor);
                        //}

                        //File.WriteAllText(@"C:\tmp_archivos\sql_debug_con_valores.txt", sqlConValores);


                        await cmd.ExecuteNonQueryAsync();
                        await tx.CommitAsync();


                    }
                }

                await conn.CloseAsync();
                
            }
            cronometer.Stop();
            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
         
        }

        //public static async Task InsertarFenalpag(List<t_fenalpag> fenal_list, int batchSize = 2000)
        //{
        //    try
        //    {
        //        if (fenal_list == null || !fenal_list.Any())
        //        {
        //            Console.WriteLine("La lista de fenalpag está vacía o es null");
        //            return;
        //        }

        //        int contador = 0;
        //        var cronometer = Stopwatch.StartNew();

        //        using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
        //        {
        //            await conn.OpenAsync();

        //            // =======================
        //            // Diccionarios en memoria
        //            // =======================
        //            var tiposDocumento = new Dictionary<string, int>();
        //            var tipoPersona = new Dictionary<string, int>();
        //            var terceros = new Dictionary<long, int>();
        //            var usuarios = new Dictionary<string, string>();
        //            var empleados = new Dictionary<string, int>();
        //            var configuracion = new Dictionary<int, int>();

                    
        //            using (var cmd = new NpgsqlCommand(@"SELECT DISTINCT descripcion,id FROM tiposdepersona", conn))
        //            using (var reader = await cmd.ExecuteReaderAsync())
        //            {
        //                while (await reader.ReadAsync())
        //                {
        //                    string descripcion = reader.GetString(0);
        //                    int id = reader.GetInt32(1);

        //                    tipoPersona[descripcion] = id;
        //                }
        //            }

        //            using (var cmd = new NpgsqlCommand(@"SELECT Id, Codigo FROM public.tiposdedocumento", conn))
        //            using (var reader = await cmd.ExecuteReaderAsync())
        //            {
        //                while (await reader.ReadAsync())
        //                {
        //                    int id = reader.GetInt32(0);
        //                    string codigo = reader.GetString(1);

        //                    tiposDocumento[codigo] = id;
        //                }
        //            }

        //            using (var cmd = new NpgsqlCommand(@"SELECT ""Identificacion"",""Id""  FROM public.""Terceros""", conn))
        //            using (var reader = await cmd.ExecuteReaderAsync())
        //            {
        //                while (reader.Read())
        //                {
        //                    long Identificacion = reader.GetInt64(0);
        //                    int id = reader.GetInt32(1);

        //                    terceros[Identificacion] = id;
        //                }
        //            }

        //            using (var cmd = new NpgsqlCommand(@"SELECT ""UserName"", ""Id"" FROM ""AspNetUsers""", conn))
        //            using (var reader = await cmd.ExecuteReaderAsync())
        //            {
        //                while (await reader.ReadAsync())
        //                {
        //                    string username = reader.GetString(0);
        //                    string id = reader.GetString(1);
        //                    usuarios[username] = (id);
        //                }
        //            }


        //            using (var cmd = new NpgsqlCommand(@"SELECT ""CodigoEmpleado"", ""Id"" FROM ""Empleados""", conn))
        //            using (var reader = await cmd.ExecuteReaderAsync())
        //            {
        //                while (await reader.ReadAsync())
        //                {
        //                    string CodigoEmpleado = reader.GetString(0);
        //                    int id = reader.GetInt32(1);
        //                    empleados[CodigoEmpleado] = (id);
        //                }
        //            }



        //            using (var cmd = new NpgsqlCommand(@"SELECT DISTINCT id FROM fenalpagconfiguracion", conn))
        //            using (var reader = await cmd.ExecuteReaderAsync())
        //            {
        //                while (await reader.ReadAsync())
        //                {

        //                    int id = reader.GetInt32(0);
        //                    configuracion[id] = (id);
        //                }
        //            }

        //            //// Tipo persona
        //            //using (var cmd = new NpgsqlCommand(@"SELECT descripcion, id FROM tiposdepersona", conn))
        //            //using (var rd = await cmd.ExecuteReaderAsync())
        //            //    while (await rd.ReadAsync())
        //            //        tipoPersona[rd.GetString(0).Trim().ToUpper()] = rd.GetInt32(1);

        //            //// Tipo documento
        //            //using (var cmd = new NpgsqlCommand(@"SELECT ""Codigo"", ""Id"" FROM public.tiposdedocumento", conn))
        //            //using (var rd = await cmd.ExecuteReaderAsync())
        //            //    while (await rd.ReadAsync())
        //            //        tiposDocumento[rd.GetString(0).Trim()] = rd.GetInt32(1);

        //            //// Terceros
        //            //using (var cmd = new NpgsqlCommand(@"SELECT ""Identificacion"", ""Id"" FROM public.""Terceros""", conn))
        //            //using (var rd = await cmd.ExecuteReaderAsync())
        //            //    while (await rd.ReadAsync())
        //            //        terceros[rd.GetInt64(0)] = rd.GetInt32(1);

        //            //// Usuarios
        //            //using (var cmd = new NpgsqlCommand(@"SELECT ""UserName"", ""Id"" FROM ""AspNetUsers""", conn))
        //            //using (var rd = await cmd.ExecuteReaderAsync())
        //            //    while (await rd.ReadAsync())
        //            //        usuarios[rd.GetString(0).Trim()] = rd.GetString(1);

        //            //// Empleados
        //            //using (var cmd = new NpgsqlCommand(@"SELECT ""CodigoEmpleado"", ""Id"" FROM ""Empleados""", conn))
        //            //using (var rd = await cmd.ExecuteReaderAsync())
        //            //    while (await rd.ReadAsync())
        //            //        empleados[rd.GetString(0).Trim()] = rd.GetInt32(1);

        //            //// Configuración
        //            //using (var cmd = new NpgsqlCommand(@"SELECT DISTINCT id FROM fenalpagconfiguracion", conn))
        //            //using (var rd = await cmd.ExecuteReaderAsync())
        //            //    while (await rd.ReadAsync())
        //            //        configuracion[rd.GetInt32(0)] = rd.GetInt32(0);

        //            // =======================
        //            // Procesamiento en lotes
        //            // =======================
        //            while (contador < fenal_list.Count)
        //            {
        //                using (var tx = await conn.BeginTransactionAsync())
        //                using (var cmd = new NpgsqlCommand())
        //                {
        //                    try
        //                    {
        //                        cmd.Connection = conn;
        //                        cmd.Transaction = tx;

        //                        var sb = new StringBuilder();
        //                        sb.AppendLine(@"
        //                INSERT INTO public.""ElementoPPYE""(
        //                    tipodedocumentoid,
        //                    tipopersonaid,
        //                    nombre1, nombre2, apellido1, apellido2,
        //                    generoid, fechanacimiento,
        //                    direccion, ciudad, telefonos, fax, email,
        //                    terceroid,
        //                    fechahoracreacion,
        //                    usuariocreaid,
        //                    numerodocumento,
        //                    estado,
        //                    observacion,
        //                    nombrecompleto,
        //                    empleadoid,
        //                    fenalpagconfiguracionid,
        //                    codigoaprobacion
        //                ) VALUES");

        //                        var valuesList = new List<string>();
        //                        int batchStartIndex = contador;
        //                        int currentBatchSize = 0;

        //                        while (contador < fenal_list.Count && currentBatchSize < batchSize)
        //                        {
        //                            valuesList.Add($@"(
        //                        @tdoc{contador},
        //                        @tipoper{contador},
        //                        @nom1{contador}, @nom2{contador}, @apl1{contador}, @apl2{contador},
        //                        @genero{contador}, @fechnac{contador},
        //                        @direccion{contador}, @ciudad{contador}, @telefono{contador}, @fax{contador}, @email{contador},
        //                        @tercero{contador},
        //                        @fechahora{contador},
        //                        @usuario{contador},
        //                        @numerodoc{contador},
        //                        @estado{contador},
        //                        @observacion{contador},
        //                        @nombrecompleto{contador},
        //                        @empleado{contador},
        //                        @config{contador},
        //                        @codigoaprob{contador}
        //                    )");

        //                            contador++;
        //                            currentBatchSize++;
        //                        }

        //                        sb.Append(string.Join(",", valuesList));
        //                        cmd.CommandText = sb.ToString();

        //                        // =======================
        //                        // Parámetros
        //                        // =======================
        //                        for (int i = 0; i < currentBatchSize; i++)
        //                        {
        //                            var f = fenal_list[batchStartIndex + i];
        //                            int idx = batchStartIndex + i;

        //                            cmd.Parameters.AddWithValue($"@tdoc{idx}",
        //                                tiposDocumento.TryGetValue(f.Tdoc, out var idTdoc) ? idTdoc : (object)DBNull.Value);

        //                            cmd.Parameters.AddWithValue($"@tipoper{idx}",
        //                                tipoPersona.TryGetValue(f.Tipo_per?.Trim().ToUpper(), out var idTp) ? idTp : (object)DBNull.Value);

        //                            cmd.Parameters.AddWithValue($"@nom1{idx}", f.Nom1 ?? "");
        //                            cmd.Parameters.AddWithValue($"@nom2{idx}", f.Nom2 ?? "");
        //                            cmd.Parameters.AddWithValue($"@apl1{idx}", f.Apl1 ?? "");
        //                            cmd.Parameters.AddWithValue($"@apl2{idx}", f.Apl2 ?? "");
        //                            cmd.Parameters.AddWithValue($"@genero{idx}", f.Sexo == "M" ? 1 : 2);
        //                            cmd.Parameters.AddWithValue($"@fechnac{idx}", (object?)f.fech_nac ?? DBNull.Value);
        //                            cmd.Parameters.AddWithValue($"@direccion{idx}", f.Direcc ?? "");
        //                            cmd.Parameters.AddWithValue($"@ciudad{idx}", f.Ciudad ?? "");
        //                            cmd.Parameters.AddWithValue($"@telefono{idx}", f.Tel ?? "");
        //                            cmd.Parameters.AddWithValue($"@fax{idx}", f.Tel ?? "");
        //                            cmd.Parameters.AddWithValue($"@email{idx}", f.Emailfe1 ?? "");

        //                            cmd.Parameters.AddWithValue($"@tercero{idx}",
        //                                f.Anexo.HasValue && terceros.TryGetValue(f.Anexo.Value, out var idTer)
        //                                    ? idTer : (object)DBNull.Value);

        //                            cmd.Parameters.AddWithValue($"@fechahora{idx}", DateTime.Now);

        //                            cmd.Parameters.AddWithValue($"@usuario{idx}",
        //                                usuarios.TryGetValue(f.Usuario, out var idUsr) ? idUsr : (object)DBNull.Value);

        //                            cmd.Parameters.AddWithValue($"@numerodoc{idx}", f.Anexo);
        //                            cmd.Parameters.AddWithValue($"@estado{idx}", f.Usado == 1 ? false : true);
        //                            cmd.Parameters.AddWithValue($"@observacion{idx}", "");
        //                            cmd.Parameters.AddWithValue($"@nombrecompleto{idx}", f.Nombre ?? "");

        //                            cmd.Parameters.AddWithValue($"@empleado{idx}",
        //                                f.Anexo.HasValue && empleados.TryGetValue(f.Anexo.Value.ToString(), out var idEmp)
        //                                    ? idEmp : (object)DBNull.Value);

        //                            cmd.Parameters.AddWithValue($"@config{idx}", configuracion.Keys.First());
        //                            cmd.Parameters.AddWithValue($"@codigoaprob{idx}", f.Autoriz ?? "");
        //                        }

        //                        await cmd.ExecuteNonQueryAsync();
        //                        await tx.CommitAsync();
        //                    }
        //                    catch (Exception exLote)
        //                    {
        //                        await tx.RollbackAsync();
        //                        Console.WriteLine($"❌ Error lote {contador - 1}: {exLote.Message}");
        //                    }
        //                }
        //            }
        //        }

        //        cronometer.Stop();
        //        Console.WriteLine($"✔ Fenalpag completado en {cronometer.Elapsed.TotalSeconds:F2} segundos");
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine("=== ERROR GENERAL ===");
        //        Console.WriteLine(ex.Message);
        //    }
        //}


        //public static void InsertarFenalpag(List<t_fenalpag> fenal_list)

        //{
        //    int contador = 0; // Contador

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        // Iniciar transacción
        //        using (var tx = conn.BeginTransaction())
        //        using (var cmd = new NpgsqlCommand())
        //        {
        //            cmd.Connection = conn;
        //            cmd.Transaction = tx;

        //            foreach (var fenalpag in fenal_list)
        //            {
        //                try
        //                {
        //                    cmd.Parameters.Clear(); // ← CRUCIA

        //                    // 1. Ejecutar TRUNCATE con CASCADE antes de insertar
        //                    //cmd.CommandText = "TRUNCATE TABLE fenalpag CASCADE;";
        //                    //cmd.ExecuteNonQuery();


        //                    // Comando SQL con parámetros
        //                    cmd.CommandText = @"
        //               INSERT INTO fenalpag (
        //                   tipodedocumentoid, tipopersonaid, nombre1, nombre2, apellido1, apellido2, generoid,
        //                   fechanacimiento, direccion, ciudad, telefonos, fax, email, terceroid,
        //                   fechahoracreacion, usuariocreaid, numerodocumento, estado, observacion, nombrecompleto,
        //                   empleadoid, fenalpagconfiguracionid, codigoaprobacion
        //               )
        //               VALUES (
        //                   (SELECT DISTINCT id FROM tiposdedocumento WHERE Codigo = @tdoc LIMIT 1),
        //                   (SELECT DISTINCT id FROM tiposdepersona WHERE descripcion = @tipo_per LIMIT 1),
        //                   @nom1, @nom2, @apl1, @apl2,
        //                   @generoid, @fech_nac, @direccion, @ciudad, @telefono, @fax, @email,
        //                   (SELECT DISTINCT ""Id"" FROM ""Terceros"" WHERE ""Identificacion"" = @anexo LIMIT 1),
        //                   @fechahora, 
        //                   (SELECT DISTINCT ""Id"" FROM ""AspNetUsers"" WHERE ""UserName"" = @usuario),
        //                   @numerodocumento, @estado, @observacion, @nombrecompleto,
        //                   (SELECT DISTINCT ""Id"" FROM ""Empleados"" WHERE ""CodigoEmpleado"" = @cod_empleado LIMIT 1),
        //                   (SELECT DISTINCT id FROM fenalpagconfiguracion WHERE id = @cod_sis),
        //                   @codigoaprobacion
        //               );";

        //                    cmd.Parameters.AddWithValue("@tdoc", fenalpag.Tdoc);
        //                    cmd.Parameters.AddWithValue("@tipo_per", fenalpag.Tipo_per.Trim().ToUpper());
        //                    cmd.Parameters.AddWithValue("@nom1", fenalpag.Nom1.Trim());
        //                    cmd.Parameters.AddWithValue("@nom2", fenalpag.Nom2.Trim());
        //                    cmd.Parameters.AddWithValue("@apl1", fenalpag.Apl1.Trim());
        //                    cmd.Parameters.AddWithValue("@apl2", fenalpag.Apl2.Trim());
        //                    cmd.Parameters.AddWithValue("@generoid", fenalpag.Sexo == "M" ? 1 : 2);
        //                    cmd.Parameters.AddWithValue("@fech_nac", fenalpag.fech_nac); // DateTime
        //                    cmd.Parameters.AddWithValue("@direccion", fenalpag.Direcc.Trim());
        //                    cmd.Parameters.AddWithValue("@ciudad", fenalpag.Ciudad.ToString());
        //                    cmd.Parameters.AddWithValue("@telefono", fenalpag.Tel.Trim());
        //                    cmd.Parameters.AddWithValue("@fax", fenalpag.Tel.Trim());
        //                    cmd.Parameters.AddWithValue("@email", fenalpag.Emailfe1.Trim());
        //                    cmd.Parameters.AddWithValue("@anexo", Convert.ToInt64(fenalpag.Anexo));
        //                    cmd.Parameters.AddWithValue("@fechahora", DateTime.Now);
        //                    cmd.Parameters.AddWithValue("@usuario", fenalpag.Usuario.Trim());
        //                    cmd.Parameters.AddWithValue("@numerodocumento", fenalpag.Anexo);
        //                    cmd.Parameters.AddWithValue("@estado", fenalpag.Usado == 1 ? false : true);
        //                    cmd.Parameters.AddWithValue("@observacion", ""); // vacío
        //                    cmd.Parameters.AddWithValue("@nombrecompleto", fenalpag.Nombre.Trim());
        //                    cmd.Parameters.AddWithValue("@cod_empleado", fenalpag.Anexo.ToString());
        //                    cmd.Parameters.AddWithValue("@cod_sis", Convert.ToInt32(fenalpag.Cod_sis));
        //                    cmd.Parameters.AddWithValue("@codigoaprobacion", fenalpag.Autoriz.Trim());

        //                    // Ejecutar el comando de inserción
        //                    cmd.ExecuteNonQuery();



        //                    contador++; // Incrementar contador
        //                    Console.WriteLine($"Cliente insertado #{contador}"); // Mostrar en consola
        //                }
        //                catch (Exception ex)
        //                {
        //                    Console.WriteLine($"⚠️ Error al insertar cliente con documento: {ex.Message}");
        //                    //ex.InnerException
        //                }
        //            }

        //            // Confirmar la transacción
        //            tx.Commit();
        //        }
        //    }

        //    Console.WriteLine("Datos insertados correctamente.");

        //}


        //public static void InsertarFenalpag(List<t_fenalpag> fenal_list)
        //{
        //    int contador = 0;
        //    var cronometer = Stopwatch.StartNew();
        //    Stopwatch sw = new Stopwatch();
        //    sw.Start();

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        foreach (var fenalpag in fenal_list)
        //        {
        //            using (var cmd = new NpgsqlCommand())
        //            {
        //                cmd.Connection = conn;

        //                try
        //                {
        //                    cmd.Parameters.Clear();

        //                    cmd.CommandText = @"
        //                INSERT INTO fenalpag (
        //                    tipodedocumentoid, tipopersonaid, nombre1, nombre2, apellido1, apellido2, generoid,
        //                    fechanacimiento, direccion, ciudad, telefonos, fax, email, terceroid,
        //                    fechahoracreacion, usuariocreaid, numerodocumento, estado, observacion, nombrecompleto,
        //                    empleadoid, fenalpagconfiguracionid, codigoaprobacion
        //                )
        //                VALUES (
        //                    (SELECT DISTINCT id FROM tiposdedocumento WHERE Codigo = @tdoc LIMIT 1),
        //                    (SELECT DISTINCT id FROM tiposdepersona WHERE descripcion = @tipo_per LIMIT 1),
        //                    @nom1, @nom2, @apl1, @apl2,
        //                    @generoid, @fech_nac, @direccion, @ciudad, @telefono, @fax, @email,
        //                    (SELECT DISTINCT ""Id"" FROM ""Terceros"" WHERE ""Identificacion"" = @anexo LIMIT 1),
        //                    @fechahora, 
        //                    (SELECT DISTINCT ""Id"" FROM ""AspNetUsers"" WHERE ""UserName"" = @usuario),
        //                    @numerodocumento, @estado, @observacion, @nombrecompleto,
        //                    (SELECT DISTINCT ""Id"" FROM ""Empleados"" WHERE ""CodigoEmpleado"" = @cod_empleado LIMIT 1),
        //                    (SELECT DISTINCT id FROM fenalpagconfiguracion WHERE id = @cod_sis),
        //                    @codigoaprobacion
        //                );";

        //                    cmd.Parameters.AddWithValue("@tdoc", fenalpag.Tdoc);
        //                    cmd.Parameters.AddWithValue("@tipo_per", fenalpag.Tipo_per.Trim().ToUpper());
        //                    cmd.Parameters.AddWithValue("@nom1", fenalpag.Nom1.Trim());
        //                    cmd.Parameters.AddWithValue("@nom2", fenalpag.Nom2.Trim());
        //                    cmd.Parameters.AddWithValue("@apl1", fenalpag.Apl1.Trim());
        //                    cmd.Parameters.AddWithValue("@apl2", fenalpag.Apl2.Trim());
        //                    cmd.Parameters.AddWithValue("@generoid", fenalpag.Sexo == "M" ? 1 : 2);
        //                    cmd.Parameters.AddWithValue("@fech_nac", fenalpag.fech_nac);
        //                    cmd.Parameters.AddWithValue("@direccion", fenalpag.Direcc.Trim());
        //                    cmd.Parameters.AddWithValue("@ciudad", fenalpag.Ciudad.ToString());
        //                    cmd.Parameters.AddWithValue("@telefono", fenalpag.Tel.Trim());
        //                    cmd.Parameters.AddWithValue("@fax", fenalpag.Tel.Trim());
        //                    cmd.Parameters.AddWithValue("@email", fenalpag.Emailfe1.Trim());
        //                    cmd.Parameters.AddWithValue("@anexo", Convert.ToInt64(fenalpag.Anexo));
        //                    cmd.Parameters.AddWithValue("@fechahora", DateTime.Now);
        //                    cmd.Parameters.AddWithValue("@usuario", fenalpag.Usuario.Trim());
        //                    cmd.Parameters.AddWithValue("@numerodocumento", fenalpag.Anexo);
        //                    cmd.Parameters.AddWithValue("@estado", fenalpag.Usado == 1 ? false : true);
        //                    cmd.Parameters.AddWithValue("@observacion", "");
        //                    cmd.Parameters.AddWithValue("@nombrecompleto", fenalpag.Nombre.Trim());
        //                    cmd.Parameters.AddWithValue("@cod_empleado", fenalpag.Anexo.ToString());
        //                    cmd.Parameters.AddWithValue("@cod_sis", Convert.ToInt32(fenalpag.Cod_sis));
        //                    cmd.Parameters.AddWithValue("@codigoaprobacion", fenalpag.Autoriz.Trim());

        //                    cmd.ExecuteNonQuery();
        //                    conn.Close();
        //                    contador++;
        //                    Console.WriteLine($"✅ Fenalpag insertado #{contador} (Doc: {fenalpag.Anexo})");
        //                }
        //                catch (Exception ex)
        //                {
        //                    Console.WriteLine($"⚠️ Error al insertar Fenalpag (Doc: {fenalpag.Anexo}): {ex.Message}");
        //                }
        //            }
        //        }
        //    }

        //    Console.WriteLine("✅ Proceso Fenalpag finalizado.");
        //    cronometer.Stop();
        //    TimeSpan tiempoTranscurrido = cronometer.Elapsed;
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
        //}

        public static async Task InsertarFenalpag(List<t_fenalpag> fenal_list, List<t_sisliqcre> sisliqcre_list, int batchSize = 2000)
        {
            try
            {
                if (fenal_list == null || !fenal_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Diccionarios en memoria
                    var tiposdedocumento = new Dictionary<string, int>();
                    var tipoPersona = new Dictionary<string, int>();
                    var tercero = new Dictionary<long, int>();
                    var usuarios = new Dictionary<string, string>();
                    var empleado = new Dictionary<string, int>();
                    var configuracion = new Dictionary<string, int>();

                    // Movimientos (IdDocumento + Numero)
                    using (var cmd = new NpgsqlCommand(@"SELECT DISTINCT descripcion,id FROM tiposdepersona", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string descripcion = reader.GetString(0).Trim();
                            int id = reader.GetInt32(1);

                            tipoPersona[descripcion] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT Id, Codigo FROM public.tiposdedocumento", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1).Trim();

                            tiposdedocumento[codigo] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Identificacion"",""Id""  FROM public.""Terceros""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (reader.Read())
                        {
                            long Identificacion = reader.GetInt64(0);
                            int id = reader.GetInt32(1);

                            tercero[Identificacion] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""UserName"", ""Id"" FROM ""AspNetUsers""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string username = reader.GetString(0).Trim();
                            string id = reader.GetString(1);
                            usuarios[username] = (id);
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT ""CodigoEmpleado"", ""Id"" FROM ""Empleados""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string CodigoEmpleado = reader.GetString(0).Trim();
                            int id = reader.GetInt32(1);
                            empleado[CodigoEmpleado] = (id);
                        }
                    }



                    using (var cmd = new NpgsqlCommand(@"SELECT descripcion, id FROM fenalpagconfiguracion", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {   
                            string descripcion = reader.GetString(0).Trim();
                            int id = reader.GetInt32(1);
                            configuracion[descripcion] = (id);
                        }
                    }

                    //SELECT "Id", "Categoria"

                    // Procesar en lotes
                    while (contador < fenal_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""fenalpag""(
                                tipodedocumentoid, tipopersonaid, nombre1, nombre2, apellido1, apellido2, generoid,
                                fechanacimiento, direccion, ciudad, telefonos, fax, email, terceroid,
                                fechahoracreacion, usuariocreaid, numerodocumento, estado, observacion, nombrecompleto,
                                empleadoid, fenalpagconfiguracionid, codigoaprobacion
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < fenal_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = fenal_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(

                                       @tdoc{contador},
                                       @tipo_per{contador},
                                       @nom1{contador}, @nom2{contador}, @apl1{contador}, @apl2{contador},
                                       @generoid{contador}, @fech_nac{contador}, @direccion{contador}, @ciudad{contador}, @telefono{contador}, @fax{contador}, @email{contador},
                                       @anexo{contador},
                                       @fechahora{contador},
                                       @usuario{contador},
                                       @numerodocumento{contador}, @estado{contador}, @observacion{contador}, @nombrecompleto{contador},
                                       @cod_empleado{contador},
                                       @cod_sis{contador},
                                       @codigoaprobacion{contador}


                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var fenalpag = fenal_list[batchStartIndex + i];

                                    string user = "CARLOSM";
                                    if (fenalpag == null) continue;


                                    cmd.Parameters.AddWithValue($"@tdoc{contador - currentBatchSize + i}",
                                    tiposdedocumento.ContainsKey(fenalpag.Tdoc.ToString())
                                    ? tiposdedocumento[fenalpag.Tdoc.ToString()]
                                    : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@tipo_per{contador - currentBatchSize + i}",
                                    tipoPersona.ContainsKey(fenalpag.Tipo_per.Trim().ToUpper())
                                    ? tipoPersona[fenalpag.Tipo_per.Trim().ToUpper()]
                                    : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@nom1{batchStartIndex + i}", fenalpag.Nom1.Trim());
                                    cmd.Parameters.AddWithValue($"@nom2{batchStartIndex + i}", fenalpag.Nom2.Trim());
                                    cmd.Parameters.AddWithValue($"@apl1{batchStartIndex + i}", fenalpag.Apl1.Trim());
                                    cmd.Parameters.AddWithValue($"@apl2{batchStartIndex + i}", fenalpag.Apl2.Trim());
                                    cmd.Parameters.AddWithValue($"@generoid{batchStartIndex + i}", fenalpag.Sexo == "M" ? 1 : 2);
                                    cmd.Parameters.AddWithValue($"@fech_nac{batchStartIndex + i}", fenalpag.fech_nac);
                                    cmd.Parameters.AddWithValue($"@direccion{batchStartIndex + i}", fenalpag.Direcc.Trim());
                                    cmd.Parameters.AddWithValue($"@ciudad{batchStartIndex + i}", fenalpag.Ciudad.ToString());
                                    cmd.Parameters.AddWithValue($"@telefono{batchStartIndex + i}", fenalpag.Tel.Trim());
                                    cmd.Parameters.AddWithValue($"@fax{batchStartIndex + i}", fenalpag.Tel.Trim());
                                    cmd.Parameters.AddWithValue($"@email{batchStartIndex + i}", fenalpag.Emailfe1.Trim());
                                    cmd.Parameters.AddWithValue($"@anexo{batchStartIndex + i}",
                                   (fenalpag.Anexo.HasValue && tercero.TryGetValue(fenalpag.Anexo.Value, out var idT))
                                       ? (object)idT
                                       : DBNull.Value
                                    );


                                    cmd.Parameters.AddWithValue($"@fechahora{batchStartIndex + i}", DateTime.Now);
                                    cmd.Parameters.AddWithValue($"@usuario{batchStartIndex + i}", fenalpag.Usuario.Trim());
                                    cmd.Parameters.AddWithValue($"@numerodocumento{batchStartIndex + i}", fenalpag.Anexo);
                                    cmd.Parameters.AddWithValue($"@estado{batchStartIndex + i}", fenalpag.Usado == 1 ? false : true);
                                    cmd.Parameters.AddWithValue($"@observacion{batchStartIndex + i}", "");
                                    cmd.Parameters.AddWithValue($"@nombrecompleto{batchStartIndex + i}", fenalpag.Nombre.Trim());
                                    // cmd.Parameters.AddWithValue("@cod_empleado", fenalpag.Anexo.ToString());

                                    cmd.Parameters.AddWithValue(
                                       $"@cod_empleado{batchStartIndex + i}",
                                       (fenalpag.Anexo.HasValue && empleado.TryGetValue(fenalpag.Anexo.Value.ToString(), out var idA))
                                           ? (object)idA
                                           : DBNull.Value
                                   );

                                    // configuracion
                                    var config_list = sisliqcre_list.FirstOrDefault(x => x.cod_sis == fenalpag.Cod_sis);

                                    if (!configuracion.TryGetValue(config_list.nombre.Trim(), out var xIdconfig))
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({xIdconfig})");
                                    }

                                    cmd.Parameters.AddWithValue($"@cod_sis{batchStartIndex + i}", xIdconfig);
                                    cmd.Parameters.AddWithValue($"@codigoaprobacion{batchStartIndex + i}", fenalpag.Autoriz.Trim());


                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{fenal_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static void InsertarMovnom(List<t_movnom> movnomList)
        {
            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                // Iniciar transacción
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    foreach (var movnom in movnomList)
                    {
                        try
                        {
                            
                            cmd.Parameters.Clear(); // ← CRUCIA
                            

                            cmd.CommandText = @"
                            INSERT INTO nominaprocesoliquidacion(
                                empleadoid, numerodedocumento, primerapellido, segundoapelliedo,
                                primernombre, segundonombre, conceptosnominaid, periodoliquidacionnominaid,
                                periodoliquidacionnominadescripcion, estado,conceptosnominadescripcion,fechahoraliquidacion,
                                devengado, descuento
                            )
                            VALUES (
                                (SELECT DISTINCT ""Id"" FROM ""Empleados"" WHERE ""CodigoEmpleado"" = @cod_empleado LIMIT 1),
                                @documento, @apellido1, @apellido2, @nombre1, @nombre2,
                                (SELECT DISTINCT id FROM conceptosnomina WHERE codigo = @concepto LIMIT 1),
                                (SELECT DISTINCT ""Id"" FROM ""PeriodoLiquidacionNomina"" WHERE ""Periodo"" = @periodo LIMIT 1),
                                (SELECT DISTINCT ""Descripcion"" FROM ""PeriodoLiquidacionNomina"" WHERE ""Periodo"" = @periodo LIMIT 1),
                                @estado, 
                                (SELECT DISTINCT descripcion FROM conceptosnomina WHERE codigo = @concepto LIMIT 1),
                                @fechahora, @devengado, @descuento
                            );";

                            // Asignación de parámetros
                            cmd.Parameters.AddWithValue("@cod_empleado", movnom.Empleado ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@documento", string.IsNullOrWhiteSpace(movnom.Cedula) ? DBNull.Value : movnom.Cedula.Trim());
                            cmd.Parameters.AddWithValue("@apellido1", string.IsNullOrWhiteSpace(movnom.Apellido1) ? DBNull.Value : movnom.Apellido1.Trim());
                            cmd.Parameters.AddWithValue("@apellido2", string.IsNullOrWhiteSpace(movnom.Apellido2) ? DBNull.Value : movnom.Apellido2.Trim());
                            cmd.Parameters.AddWithValue("@nombre1", string.IsNullOrWhiteSpace(movnom.Nombre1) ? DBNull.Value : movnom.Nombre1.Trim());
                            cmd.Parameters.AddWithValue("@nombre2", string.IsNullOrWhiteSpace(movnom.Nombre2) ? DBNull.Value : movnom.Nombre2.Trim());
                            cmd.Parameters.AddWithValue("@concepto", movnom.Concepto.ToString().Trim());
                            cmd.Parameters.AddWithValue("@periodo", string.IsNullOrWhiteSpace(movnom.Periodo)? (object)DBNull.Value :Convert.ToInt32(movnom.Periodo));
                            cmd.Parameters.AddWithValue("@estado", true);
                            cmd.Parameters.AddWithValue("@fechahora", movnom.fecha); // DateTime
                            cmd.Parameters.AddWithValue("@devengado", movnom.Devengado); // A ajustar si tienes este dato
                            cmd.Parameters.AddWithValue("@descuento", movnom.Descuento); // A ajustar si tienes este dato

                            string finalQuery = GetInterpolatedQuery(cmd);
                            Console.WriteLine("SQL con valores:");
                            Console.WriteLine(finalQuery);



                            // Ejecutar el comando de inserción 
                            cmd.ExecuteNonQuery();


                            contador++; // Incrementar contador
                            Console.WriteLine($"Registro insertado #{contador}"); // Mostrar en consola
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar registro: {ex.Message}");
                            //ex.InnerException
                        }
                    }

                    // Confirmar la transacción
                    tx.Commit();
                }
            }

            Console.WriteLine("Datos insertados correctamente.");
        }

        public static async Task InsertarPlanDet(List<t_planDet> PlanDetList, int batchSize = 3000)
        {
            try
            {
                if (PlanDetList == null || !PlanDetList.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Diccionarios en memoria
                    
                    var usuarios = new Dictionary<string, string>();
                    var empleado = new Dictionary<string, int>();
                    var festivos = new Dictionary<DateOnly, int>();


                    using (var cmd = new NpgsqlCommand(@"SELECT ""CodigoEmpleado"", ""Id"" FROM ""Empleados""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string CodigoEmpleado = reader.GetString(0);
                            int id = reader.GetInt32(1);
                            empleado[CodigoEmpleado] = (id);
                        }
                    }
                    //SELECT id, vigencia, periodo, dia, estado FROM public."DiasFestivos";

                    using (var cmd = new NpgsqlCommand(@"SELECT dia, id FROM ""DiasFestivos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            DateOnly dia = DateOnly.FromDateTime(reader.GetDateTime(0));
                            int id = reader.GetInt32(1);

                            festivos[dia] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""UserName"" FROM ""AspNetUsers""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string id = reader.GetString(0);       // Id en AspNetUsers casi siempre es string
                            string username = reader.GetString(1); // UserName
                            usuarios[username] = id;
                        }
                    }


                    //SELECT "Id", "Categoria"

                    // Procesar en lotes
                    while (contador < PlanDetList.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.planillabiometricadomingosyfestivos(
                                dia, estado, empleadoid, fechahoracreacion, usuarioidcreacion, numeroplanilla, diasfestivosid
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < PlanDetList.Count && currentBatchSize < batchSize)
                                {
                                    var mov = PlanDetList[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                       

                                        @dia{contador},
                                        @estado{contador},
                                        @empleadoid{contador},
                                        @fechahoracreacion{contador},
                                        @usuarioidcreacion{contador},
                                        @numeroplanilla{contador},
                                        @diasfestivosid{contador}
   

                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var planDet = PlanDetList[batchStartIndex + i];

                                    string user = "CARLOSM";
                                    if (planDet == null) continue;


                                 

                                    cmd.Parameters.AddWithValue($"@dia{batchStartIndex + i}", planDet.Fecha);
                                    cmd.Parameters.AddWithValue($"@estado{batchStartIndex + i}", 1);
                                    cmd.Parameters.AddWithValue(
                                        $"@empleadoid{batchStartIndex + i}",
                                        (!string.IsNullOrWhiteSpace(planDet.Empleado) &&
                                         empleado.TryGetValue(planDet.Empleado, out var idA))
                                            ? (object)idA
                                            : DBNull.Value
                                    );

                                    //cmd.Parameters.AddWithValue("empleadoid", planDet.Empleado.Trim());
                                    cmd.Parameters.AddWithValue($"@fechahoracreacion{batchStartIndex + i}", planDet.FechaCreacion);

                                    cmd.Parameters.AddWithValue($"@usuarioidcreacion{batchStartIndex + i}",
                                    usuarios.TryGetValue(planDet.Usa_crea, out var userId) ? userId : (object)DBNull.Value);


                                    //cmd.Parameters.AddWithValue("usuarioidcreacion", planDet.Usa_crea);
                                    cmd.Parameters.AddWithValue($"@numeroplanilla{batchStartIndex + i}", planDet.NumeroPlanilla);

                                    cmd.Parameters.AddWithValue(
                                      $"@diasfestivosid{batchStartIndex + i}",
                                      festivos.TryGetValue(planDet.Fecha, out var fes)
                                          ? fes
                                          : (object)DBNull.Value
                                  );


                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{PlanDetList.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        //public static void InsertarDcom(List<t_dcom> DcomList)
        //{
        //    int contador = 0;

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        foreach (var dcomS in DcomList)
        //        {
        //            using (var tx = conn.BeginTransaction())
        //            using (var cmd = new NpgsqlCommand())
        //            {
        //                cmd.Connection = conn;
        //                cmd.Transaction = tx;

        //                try
        //                {
        //                    cmd.CommandText = @"
        //                INSERT INTO detalleordenesdecompra(
        //                    idordenesdecompra, idproducto, cantidad, valor, estado, fechahoramovimiento,
        //                    porcentaje, descuento, sugerido, requerido, bonificacion, observacion,
        //                    sucursalid, diasinventario, iva, impuestoconsumo, talla, nombreproducto,
        //                    subgruposproducto, marcasproducto, ""tipoProducto"", valoriva, valorimpoconsumo,
        //                    valordscto, ean8, ean13, codigoreferencia, valorbonificacion
        //                )
        //                VALUES (
        //                    (SELECT DISTINCT Id FROM ordenesdecompra WHERE numeroordencompra = @idordenesdecompra LIMIT 1),
        //                    (SELECT DISTINCT ""Id"" FROM ""Productos"" WHERE ""Codigo"" = @idproducto LIMIT 1),
        //                    @cantidad, @valor, @estado, @fechahoramovimiento, @porcentaje,
        //                    @descuento, @sugerido, @requerido, @bonificacion, @observacion,
        //                    (SELECT DISTINCT id FROM ""Sucursales"" WHERE codigo = @sucursalid),
        //                    @diasinventario, @iva, @impuestoconsumo, @talla, @nombreproducto,
        //                    @subgruposproducto, @marcasproducto, @tipoProducto, @valoriva,
        //                    @valorimpoconsumo, @valordscto, @ean8, @ean13, @codigoreferencia, @valorbonificacion
        //                );";

        //                    // Limpiar y agregar parámetros
        //                    cmd.Parameters.Clear();
        //                    cmd.Parameters.AddWithValue("@idordenesdecompra", dcomS.ordenedecompra);
        //                    cmd.Parameters.AddWithValue("@idproducto", dcomS.producto);
        //                    cmd.Parameters.AddWithValue("@cantidad", dcomS.cantidad);
        //                    cmd.Parameters.AddWithValue("@valor", dcomS.valor);
        //                    cmd.Parameters.AddWithValue("@estado", true);
        //                    cmd.Parameters.AddWithValue("@fechahoramovimiento", dcomS.fechahoramovimiento);
        //                    cmd.Parameters.AddWithValue("@porcentaje", dcomS.porcentaje);
        //                    cmd.Parameters.AddWithValue("@descuento", dcomS.descuento);
        //                    cmd.Parameters.AddWithValue("@sugerido", dcomS.sugerido);
        //                    cmd.Parameters.AddWithValue("@requerido", dcomS.requerido);
        //                    cmd.Parameters.AddWithValue("@bonificacion", dcomS.bonificacion);
        //                    cmd.Parameters.AddWithValue("@observacion", dcomS.observacion ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue("@sucursalid", dcomS.sucursal);
        //                    cmd.Parameters.AddWithValue("@diasinventario", dcomS.diasinventario);
        //                    cmd.Parameters.AddWithValue("@iva", dcomS.iva);
        //                    cmd.Parameters.AddWithValue("@impuestoconsumo", dcomS.impuestoconsumo);
        //                    cmd.Parameters.AddWithValue("@talla", dcomS.talla);
        //                    cmd.Parameters.AddWithValue("@nombreproducto", dcomS.nombreproducto);
        //                    cmd.Parameters.AddWithValue("@subgruposproducto", dcomS.subgruposproducto);
        //                    cmd.Parameters.AddWithValue("@marcasproducto", dcomS.marcasproducto);
        //                    cmd.Parameters.AddWithValue("@tipoProducto", dcomS.tipoProducto);
        //                    cmd.Parameters.AddWithValue("@valoriva", dcomS.valoriva);
        //                    cmd.Parameters.AddWithValue("@valorimpoconsumo", dcomS.valorimpoconsumo);
        //                    cmd.Parameters.AddWithValue("@valordscto", dcomS.valordscto);
        //                    cmd.Parameters.AddWithValue("@ean8", dcomS.ean8 ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue("@ean13", dcomS.ean13 ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue("@codigoreferencia", dcomS.codigoreferencia);
        //                    cmd.Parameters.AddWithValue("@valorbonificacion", dcomS.valorbonificacion);

        //                    // Ejecutar e insertar
        //                    cmd.ExecuteNonQuery();
        //                    tx.Commit();

        //                    contador++;
        //                    Console.WriteLine($"✔ Registro insertado #{contador}");
        //                }
        //                catch (Exception ex)
        //                {
        //                    tx.Rollback();
        //                    Console.WriteLine($"⚠️ Error en registro #{contador + 1}: {ex.Message}");
        //                }
        //            }
        //        }

        //        Console.WriteLine("✅ Inserción finalizada.");
        //    }
        //}


        //public static void InsertarDcom(List<t_dcom> DcomList, int batchSize = 100)
        //{
        //    int contador = 0;
        //    var cronometer = Stopwatch.StartNew();

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        while (contador < DcomList.Count)
        //        {
        //            using (var tx = conn.BeginTransaction())
        //            using (var cmd = new NpgsqlCommand())
        //            {
        //                cmd.Connection = conn;
        //                cmd.Transaction = tx;

        //                var commandText = new StringBuilder();

        //                commandText.AppendLine(@"
        //        INSERT INTO detalleordenesdecompra (
        //        idordenesdecompra, idproducto, cantidad, valor, estado, fechahoramovimiento,
        //        porcentaje, descuento, sugerido, requerido, bonificacion, observacion,
        //        sucursalid, diasinventario, iva, impuestoconsumo, talla, nombreproducto,
        //        subgruposproducto, marcasproducto, ""tipoProducto"", valoriva, valorimpoconsumo,
        //        valordscto, ean8, ean13, codigoreferencia, valorbonificacion
        //        ) VALUES ");

        //                var valuesList = new List<string>();
        //                int currentBatchSize = 0;
        //                int startIndex = contador; // Guardar el índice inicial del lote

        //                // Construir la consulta VALUES
        //                while (contador < DcomList.Count && currentBatchSize < batchSize)
        //                {
        //                    var paramIndex = currentBatchSize; // Usar índice relativo al lote

        //                    var values = $@"
        //    (
        //         (SELECT DISTINCT Id FROM ordenesdecompra WHERE numeroordencompra = @idordenesdecompra{paramIndex} LIMIT 1),
        //         (SELECT DISTINCT ""Id"" FROM ""Productos"" WHERE ""Codigo"" = @idproducto{paramIndex} LIMIT 1),
        //         @cantidad{paramIndex}, @valor{paramIndex}, @estado{paramIndex}, @fechahoramovimiento{paramIndex}, @porcentaje{paramIndex},
        //         @descuento{paramIndex}, @sugerido{paramIndex}, @requerido{paramIndex}, @bonificacion{paramIndex}, @observacion{paramIndex},
        //         (SELECT DISTINCT id FROM ""Sucursales"" WHERE codigo = @sucursalid{paramIndex}),
        //         @diasinventario{paramIndex}, @iva{paramIndex}, @impuestoconsumo{paramIndex}, @talla{paramIndex}, @nombreproducto{paramIndex},
        //         @subgruposproducto{paramIndex}, @marcasproducto{paramIndex}, @tipoProducto{paramIndex}, @valoriva{paramIndex},
        //         @valorimpoconsumo{paramIndex}, @valordscto{paramIndex}, @ean8{paramIndex}, @ean13{paramIndex}, @codigoreferencia{paramIndex}, @valorbonificacion{paramIndex}
        //         )";

        //                    valuesList.Add(values);
        //                    contador++;
        //                    currentBatchSize++;
        //                }

        //                // Combinar todos los VALUES en una sola consulta
        //                commandText.Append(string.Join(",", valuesList));

        //                // Asignar el comando completo
        //                cmd.CommandText = commandText.ToString();

        //                // Limpiar parámetros una sola vez antes de agregar todos los del lote
        //                cmd.Parameters.Clear();

        //                // Agregar parámetros para el lote actual
        //                for (int i = 0; i < currentBatchSize; i++)
        //                {
        //                    var dcomS = DcomList[startIndex + i];

        //                    cmd.Parameters.AddWithValue($"@idordenesdecompra{i}", dcomS.ordenedecompra);
        //                    cmd.Parameters.AddWithValue($"@idproducto{i}", dcomS.producto);
        //                    cmd.Parameters.AddWithValue($"@cantidad{i}", dcomS.cantidad);
        //                    cmd.Parameters.AddWithValue($"@valor{i}", dcomS.valor);
        //                    cmd.Parameters.AddWithValue($"@estado{i}", true);
        //                    cmd.Parameters.AddWithValue($"@fechahoramovimiento{i}", dcomS.fechahoramovimiento);
        //                    cmd.Parameters.AddWithValue($"@porcentaje{i}", dcomS.porcentaje);
        //                    cmd.Parameters.AddWithValue($"@descuento{i}", dcomS.descuento);
        //                    cmd.Parameters.AddWithValue($"@sugerido{i}", dcomS.sugerido);
        //                    cmd.Parameters.AddWithValue($"@requerido{i}", dcomS.requerido);
        //                    cmd.Parameters.AddWithValue($"@bonificacion{i}", dcomS.bonificacion);
        //                    cmd.Parameters.AddWithValue($"@observacion{i}", dcomS.observacion ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue($"@sucursalid{i}", dcomS.sucursal);
        //                    cmd.Parameters.AddWithValue($"@diasinventario{i}", dcomS.diasinventario);
        //                    cmd.Parameters.AddWithValue($"@iva{i}", dcomS.iva);
        //                    cmd.Parameters.AddWithValue($"@impuestoconsumo{i}", dcomS.impuestoconsumo);
        //                    cmd.Parameters.AddWithValue($"@talla{i}", dcomS.talla);
        //                    cmd.Parameters.AddWithValue($"@nombreproducto{i}", dcomS.nombreproducto);
        //                    cmd.Parameters.AddWithValue($"@subgruposproducto{i}", dcomS.subgruposproducto);
        //                    cmd.Parameters.AddWithValue($"@marcasproducto{i}", dcomS.marcasproducto);
        //                    cmd.Parameters.AddWithValue($"@tipoProducto{i}", dcomS.tipoProducto);
        //                    cmd.Parameters.AddWithValue($"@valoriva{i}", dcomS.valoriva);
        //                    cmd.Parameters.AddWithValue($"@valorimpoconsumo{i}", dcomS.valorimpoconsumo);
        //                    cmd.Parameters.AddWithValue($"@valordscto{i}", dcomS.valordscto);
        //                    cmd.Parameters.AddWithValue($"@ean8{i}", dcomS.ean8 ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue($"@ean13{i}", dcomS.ean13 ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue($"@codigoreferencia{i}", dcomS.codigoreferencia);
        //                    cmd.Parameters.AddWithValue($"@valorbonificacion{i}", dcomS.valorbonificacion);
        //                }

        //                Console.WriteLine(cmd.CommandText);

        //                cmd.ExecuteNonQuery();
        //                tx.Commit();
        //            }
        //        }
        //    }

        //    cronometer.Stop();
        //    TimeSpan tiempoTranscurrido = cronometer.Elapsed;
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
        //}

        public static void InsertarDcom(List<t_dcom> DcomList, int batchSize = 100)
        {
            int contador = 0;
            var cronometer = Stopwatch.StartNew();
            var errores = new List<string>();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Include Error Detail=true"))
            {
                conn.Open();

                while (contador < DcomList.Count)
                {
                    using (var tx = conn.BeginTransaction())
                    {
                        try
                        {
                            using (var cmd = new NpgsqlCommand())
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                        INSERT INTO detalleordenesdecompra (
                        idordenesdecompra, idproducto, cantidad, valor, estado, fechahoramovimiento,
                        porcentaje, descuento, sugerido, requerido, bonificacion, observacion,
                        sucursalid, diasinventario, iva, impuestoconsumo, talla, nombreproducto,
                        subgruposproducto, marcasproducto, ""tipoProducto"", valoriva, valorimpoconsumo,
                        valordscto, ean8, ean13, codigoreferencia, valorbonificacion
                        ) VALUES ");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int startIndex = contador;

                                // Construir la consulta VALUES
                                while (contador < DcomList.Count && currentBatchSize < batchSize)
                                {
                                    var paramIndex = currentBatchSize;

                                    var values = $@"
                    (
                         (SELECT DISTINCT Id FROM ordenesdecompra WHERE numeroordencompra = @idordenesdecompra{paramIndex} LIMIT 1),
                         (SELECT DISTINCT ""Id"" FROM ""Productos"" WHERE ""Codigo"" = @idproducto{paramIndex} LIMIT 1),
                         @cantidad{paramIndex}, @valor{paramIndex}, @estado{paramIndex}, @fechahoramovimiento{paramIndex}, @porcentaje{paramIndex},
                         @descuento{paramIndex}, @sugerido{paramIndex}, @requerido{paramIndex}, @bonificacion{paramIndex}, @observacion{paramIndex},
                         (SELECT DISTINCT id FROM ""Sucursales"" WHERE codigo = @sucursalid{paramIndex}),
                         @diasinventario{paramIndex}, @iva{paramIndex}, @impuestoconsumo{paramIndex}, @talla{paramIndex}, @nombreproducto{paramIndex},
                         @subgruposproducto{paramIndex}, @marcasproducto{paramIndex}, @tipoProducto{paramIndex}, @valoriva{paramIndex},
                         @valorimpoconsumo{paramIndex}, @valordscto{paramIndex}, @ean8{paramIndex}, @ean13{paramIndex}, @codigoreferencia{paramIndex}, @valorbonificacion{paramIndex}
                         )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();
                                cmd.Parameters.Clear();

                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var dcomS = DcomList[startIndex + i];

                                    cmd.Parameters.AddWithValue($"@idordenesdecompra{i}", dcomS.ordenedecompra);
                                    cmd.Parameters.AddWithValue($"@idproducto{i}", dcomS.producto);
                                    cmd.Parameters.AddWithValue($"@cantidad{i}", dcomS.cantidad);
                                    cmd.Parameters.AddWithValue($"@valor{i}", dcomS.valor);
                                    cmd.Parameters.AddWithValue($"@estado{i}", true);
                                    cmd.Parameters.AddWithValue($"@fechahoramovimiento{i}", dcomS.fechahoramovimiento);
                                    cmd.Parameters.AddWithValue($"@porcentaje{i}", dcomS.porcentaje);
                                    cmd.Parameters.AddWithValue($"@descuento{i}", dcomS.descuento);
                                    cmd.Parameters.AddWithValue($"@sugerido{i}", dcomS.sugerido);
                                    cmd.Parameters.AddWithValue($"@requerido{i}", dcomS.requerido);
                                    cmd.Parameters.AddWithValue($"@bonificacion{i}", dcomS.bonificacion);
                                    cmd.Parameters.AddWithValue($"@observacion{i}", dcomS.observacion ?? (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@sucursalid{i}", dcomS.sucursal);
                                    cmd.Parameters.AddWithValue($"@diasinventario{i}", dcomS.diasinventario);
                                    cmd.Parameters.AddWithValue($"@iva{i}", dcomS.iva);
                                    cmd.Parameters.AddWithValue($"@impuestoconsumo{i}", dcomS.impuestoconsumo);
                                    cmd.Parameters.AddWithValue($"@talla{i}", dcomS.talla);
                                    cmd.Parameters.AddWithValue($"@nombreproducto{i}", dcomS.nombreproducto);
                                    cmd.Parameters.AddWithValue($"@subgruposproducto{i}", dcomS.subgruposproducto);
                                    cmd.Parameters.AddWithValue($"@marcasproducto{i}", dcomS.marcasproducto);
                                    cmd.Parameters.AddWithValue($"@tipoProducto{i}", dcomS.tipoProducto);
                                    cmd.Parameters.AddWithValue($"@valoriva{i}", dcomS.valoriva);
                                    cmd.Parameters.AddWithValue($"@valorimpoconsumo{i}", dcomS.valorimpoconsumo);
                                    cmd.Parameters.AddWithValue($"@valordscto{i}", dcomS.valordscto);
                                    cmd.Parameters.AddWithValue($"@ean8{i}", dcomS.ean8 ?? (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@ean13{i}", dcomS.ean13 ?? (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@codigoreferencia{i}", dcomS.codigoreferencia);
                                    cmd.Parameters.AddWithValue($"@valorbonificacion{i}", dcomS.valorbonificacion);
                                }

                                cmd.ExecuteNonQuery();
                                tx.Commit();

                                Console.WriteLine($"Lote procesado exitosamente. Registros {startIndex + 1} al {contador}");
                            }
                        }
                        catch (Exception ex)
                        {
                            tx.Rollback();
                            Console.WriteLine($"Error en lote {contador + 1} al {contador}: {ex.Message}");

                            // Procesar registros individualmente para identificar el problemático
                            ProcesarIndividualmente(conn, DcomList, contador, contador - contador, errores);
                        }
                    }
                }
            }

            cronometer.Stop();
            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");

            if (errores.Any())
            {
                Console.WriteLine("\n=== ERRORES ENCONTRADOS ===");
                foreach (var error in errores)
                {
                    Console.WriteLine(error);
                }
            }
        }

        private static void ProcesarIndividualmente(NpgsqlConnection conn, List<t_dcom> DcomList, int startIndex, int count, List<string> errores)
        {
            for (int i = 0; i < count; i++)
            {
                var index = startIndex + i;
                var dcomS = DcomList[index];

                try
                {
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tx;

                        // Primero verificar si existen las referencias
                        var verificaciones = VerificarReferencias(conn, dcomS, index);

                        if (!string.IsNullOrEmpty(verificaciones))
                        {
                            errores.Add($"Registro {index + 1}: {verificaciones}");
                            Console.WriteLine($"Error en registro {index + 1}: {verificaciones}");
                            continue;
                        }

                        cmd.CommandText = @"
                INSERT INTO detalleordenesdecompra (
                idordenesdecompra, idproducto, cantidad, valor, estado, fechahoramovimiento,
                porcentaje, descuento, sugerido, requerido, bonificacion, observacion,
                sucursalid, diasinventario, iva, impuestoconsumo, talla, nombreproducto,
                subgruposproducto, marcasproducto, ""tipoProducto"", valoriva, valorimpoconsumo,
                valordscto, ean8, ean13, codigoreferencia, valorbonificacion
                ) VALUES (
                 (SELECT DISTINCT Id FROM ordenesdecompra WHERE numeroordencompra = @idordenesdecompra LIMIT 1),
                 (SELECT DISTINCT ""Id"" FROM ""Productos"" WHERE ""Codigo"" = @idproducto LIMIT 1),
                 @cantidad, @valor, @estado, @fechahoramovimiento, @porcentaje,
                 @descuento, @sugerido, @requerido, @bonificacion, @observacion,
                 (SELECT DISTINCT id FROM ""Sucursales"" WHERE codigo = @sucursalid),
                 @diasinventario, @iva, @impuestoconsumo, @talla, @nombreproducto,
                 @subgruposproducto, @marcasproducto, @tipoProducto, @valoriva,
                 @valorimpoconsumo, @valordscto, @ean8, @ean13, @codigoreferencia, @valorbonificacion
                 )";

                        cmd.Parameters.AddWithValue("@idordenesdecompra", dcomS.ordenedecompra);
                        cmd.Parameters.AddWithValue("@idproducto", dcomS.producto);
                        cmd.Parameters.AddWithValue("@cantidad", dcomS.cantidad);
                        cmd.Parameters.AddWithValue("@valor", dcomS.valor);
                        cmd.Parameters.AddWithValue("@estado", true);
                        cmd.Parameters.AddWithValue("@fechahoramovimiento", dcomS.fechahoramovimiento);
                        cmd.Parameters.AddWithValue("@porcentaje", dcomS.porcentaje);
                        cmd.Parameters.AddWithValue("@descuento", dcomS.descuento);
                        cmd.Parameters.AddWithValue("@sugerido", dcomS.sugerido);
                        cmd.Parameters.AddWithValue("@requerido", dcomS.requerido);
                        cmd.Parameters.AddWithValue("@bonificacion", dcomS.bonificacion);
                        cmd.Parameters.AddWithValue("@observacion", dcomS.observacion ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@sucursalid", dcomS.sucursal);
                        cmd.Parameters.AddWithValue("@diasinventario", dcomS.diasinventario);
                        cmd.Parameters.AddWithValue("@iva", dcomS.iva);
                        cmd.Parameters.AddWithValue("@impuestoconsumo", dcomS.impuestoconsumo);
                        cmd.Parameters.AddWithValue("@talla", dcomS.talla);
                        cmd.Parameters.AddWithValue("@nombreproducto", dcomS.nombreproducto);
                        cmd.Parameters.AddWithValue("@subgruposproducto", dcomS.subgruposproducto);
                        cmd.Parameters.AddWithValue("@marcasproducto", dcomS.marcasproducto);
                        cmd.Parameters.AddWithValue("@tipoProducto", dcomS.tipoProducto);
                        cmd.Parameters.AddWithValue("@valoriva", dcomS.valoriva);
                        cmd.Parameters.AddWithValue("@valorimpoconsumo", dcomS.valorimpoconsumo);
                        cmd.Parameters.AddWithValue("@valordscto", dcomS.valordscto);
                        cmd.Parameters.AddWithValue("@ean8", dcomS.ean8 ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@ean13", dcomS.ean13 ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@codigoreferencia", dcomS.codigoreferencia);
                        cmd.Parameters.AddWithValue("@valorbonificacion", dcomS.valorbonificacion);

                        cmd.ExecuteNonQuery();
                        tx.Commit();
                        Console.WriteLine($"Registro {index + 1} procesado exitosamente");
                    }
                }
                catch (Exception ex)
                {
                    var errorMsg = $"Registro {index + 1} - Producto: {dcomS.producto}, Orden: {dcomS.ordenedecompra}, Sucursal: {dcomS.sucursal} - Error: {ex.Message}";
                    errores.Add(errorMsg);
                    Console.WriteLine($"Error: {errorMsg}");
                }
            }
        }

        private static string VerificarReferencias(NpgsqlConnection conn, t_dcom dcom, int index)
        {
            var errores = new List<string>();

            try
            {
                // Verificar orden de compra
                using (var cmd = new NpgsqlCommand("SELECT COUNT(*) FROM ordenesdecompra WHERE numeroordencompra = @orden", conn))
                {
                    cmd.Parameters.AddWithValue("@orden", dcom.ordenedecompra);
                    var count = Convert.ToInt32(cmd.ExecuteScalar());
                    if (count == 0)
                    {
                        errores.Add($"Orden de compra '{dcom.ordenedecompra}' no existe");
                    }
                }

                // Verificar producto
                using (var cmd = new NpgsqlCommand("SELECT COUNT(*) FROM \"Productos\" WHERE \"Codigo\" = @producto", conn))
                {
                    cmd.Parameters.AddWithValue("@producto", dcom.producto);
                    var count = Convert.ToInt32(cmd.ExecuteScalar());
                    if (count == 0)
                    {
                        errores.Add($"Producto '{dcom.producto}' no existe");
                    }
                }

                // Verificar sucursal
                using (var cmd = new NpgsqlCommand("SELECT COUNT(*) FROM \"Sucursales\" WHERE codigo = @sucursal", conn))
                {
                    cmd.Parameters.AddWithValue("@sucursal", dcom.sucursal);
                    var count = Convert.ToInt32(cmd.ExecuteScalar());
                    if (count == 0)
                    {
                        errores.Add($"Sucursal '{dcom.sucursal}' no existe");
                    }
                }

                // Verificar si el producto devuelve NULL
                using (var cmd = new NpgsqlCommand("SELECT \"Id\" FROM \"Productos\" WHERE \"Codigo\" = @producto LIMIT 1", conn))
                {
                    cmd.Parameters.AddWithValue("@producto", dcom.producto);
                    var result = cmd.ExecuteScalar();
                    if (result == null || result == DBNull.Value)
                    {
                        errores.Add($"Producto '{dcom.producto}' devuelve NULL");
                    }
                }

                // Verificar si la orden devuelve NULL
                using (var cmd = new NpgsqlCommand("SELECT Id FROM ordenesdecompra WHERE numeroordencompra = @orden LIMIT 1", conn))
                {
                    cmd.Parameters.AddWithValue("@orden", dcom.ordenedecompra);
                    var result = cmd.ExecuteScalar();
                    if (result == null || result == DBNull.Value)
                    {
                        errores.Add($"Orden '{dcom.ordenedecompra}' devuelve NULL");
                    }
                }

                // Verificar si la sucursal devuelve NULL
                using (var cmd = new NpgsqlCommand("SELECT id FROM \"Sucursales\" WHERE codigo = @sucursal LIMIT 1", conn))
                {
                    cmd.Parameters.AddWithValue("@sucursal", dcom.sucursal);
                    var result = cmd.ExecuteScalar();
                    if (result == null || result == DBNull.Value)
                    {
                        errores.Add($"Sucursal '{dcom.sucursal}' devuelve NULL");
                    }
                }
            }
            catch (Exception ex)
            {
                errores.Add($"Error verificando referencias: {ex.Message}");
            }

            return string.Join(", ", errores);
        }

        //public static void InsertarCreditos(List<t_fenalCredtos> creditosList)
        //{
        //    int contador = 0; // Contador
        //    string xEstado = "";
        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        // Iniciar transacción
        //        using (var tx = conn.BeginTransaction())
        //        using (var cmd = new NpgsqlCommand())
        //        {
        //            cmd.Connection = conn;
        //            cmd.Transaction = tx;

        //            // Insertar cada registro de vended
        //            foreach (var creditos in creditosList)
        //            {


        //                try
        //                {

        //                    cmd.Parameters.Clear();
        //                    cmd.CommandText = @"
        //                    INSERT INTO public.fenalpagcreditos(
        //                     valormercanciacliente, cantidadcuotascliente, fenalpagid, tasainteres, iva, porcentajecuotainicial,
        //                     numerocuotas, valorlimitecredito, idformapago, porcentajeaval, numerocuotacobrainteres,
        //                     valorminimocredito, estado, valorcuotainicialcliente, porcentajecuotainicialcliente, periodicidad)
        //                    VALUES (
        //                     @valormercanciacliente, @cantidadcuotascliente,
        //                        (SELECT id FROM fenalpag WHERE numerodocumento = @fenalpagid LIMIT 1), 
        //                        @tasainteres, @iva, @porcentajecuotainicial,
        //                     @numerocuotas, @valorlimitecredito,
        //                        (SELECT ""Id"" FROM ""FormasPago"" where ""Codigo"" = @idformapago LIMIT 1),
        //                        @porcentajeaval, @numerocuotacobrainteres,
        //                     @valorminimocredito, @estado, @valorcuotainicialcliente, @porcentajecuotainicialcliente, @periodicidad
        //                    )";

        //                    switch(creditos.periodicidad)
        //                    {
        //                        case 1:

        //                            xEstado = "Quincenal";

        //                            break;
        //                        case 2:

        //                            xEstado = "Mensual";

        //                            break;
        //                        case 3:

        //                            xEstado = "Semanal";

        //                            break;
        //                    }




        //                    // Parametrización de la consulta
        //                    //cmd.Parameters.Clear();  // Limpiar los parámetros antes de agregar nuevos
        //                    cmd.Parameters.AddWithValue("valormercanciacliente", creditos.valormercanciacliente);
        //                    cmd.Parameters.AddWithValue("cantidadcuotascliente", creditos.cantidadcuotascliente);
        //                    cmd.Parameters.AddWithValue("fenalpagid", creditos.fenalpagid.ToString());
        //                    cmd.Parameters.AddWithValue("tasainteres", creditos.tasainteres);
        //                    cmd.Parameters.AddWithValue("iva", creditos.iva);
        //                    cmd.Parameters.AddWithValue("porcentajecuotainicial", creditos.porcentajecuotainicial);
        //                    cmd.Parameters.AddWithValue("numerocuotas", creditos.numerocuotas);
        //                    cmd.Parameters.AddWithValue("valorlimitecredito", 0);
        //                    cmd.Parameters.AddWithValue("idformapago", creditos.idformapago.ToString());
        //                    cmd.Parameters.AddWithValue("porcentajeaval", creditos.porcentajeaval);
        //                    cmd.Parameters.AddWithValue("numerocuotacobrainteres",0);
        //                    cmd.Parameters.AddWithValue("valorminimocredito", 0);
        //                    cmd.Parameters.AddWithValue("estado", true);
        //                    cmd.Parameters.AddWithValue("valorcuotainicialcliente", creditos.valorcuotainicialcliente);
        //                    cmd.Parameters.AddWithValue("porcentajecuotainicialcliente", creditos.porcentajecuotainicialcliente);
        //                    cmd.Parameters.AddWithValue("periodicidad", xEstado);

        //                    string finalQuery = GetInterpolatedQuery(cmd);
        //                    Console.WriteLine("SQL con valores:");
        //                    Console.WriteLine(finalQuery);

        //                    // Ejecutar el comando de inserción 
        //                    cmd.ExecuteNonQuery();


        //                    contador++; // Incrementar contador
        //                    Console.WriteLine($"Registro insertado #{contador}"); // Mostrar en consola
        //                }
        //                catch (Exception ex)
        //                {
        //                    Console.WriteLine($"⚠️ Error al insertar registro: {ex.Message}");
        //                    conn.Close();
        //                    //ex.InnerException
        //                }

        //            }

        //            // Confirmar la transacción
        //            tx.Commit();
        //        }
        //    }

        //    Console.WriteLine("Datos insertados correctamente.");
        //}

        public static void InsertarCreditos(List<t_fenalCredtos> creditosList, int batchSize = 1000)
        {
            int contador = 0;
            string xEstado = "";
            var cronometer = Stopwatch.StartNew();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();
                while (contador < creditosList.Count)
                {
                    using var tx = conn.BeginTransaction();
                    using (var cmd = new NpgsqlCommand())
                    {
                        try
                        {
                            cmd.Connection = conn;
                            cmd.Transaction = tx;

                            var commandText = new StringBuilder();

                            commandText.AppendLine(@"
                                 INSERT INTO public.fenalpagcreditos(
                                    valormercanciacliente, cantidadcuotascliente, fenalpagid, tasainteres, iva, porcentajecuotainicial,
                                    numerocuotas, valorlimitecredito, idformapago, porcentajeaval, numerocuotacobrainteres,
                                    valorminimocredito, estado, valorcuotainicialcliente, porcentajecuotainicialcliente, periodicidad)
                                 VALUES");

                            var valuesList = new List<string>();
                            int currentBatchSize = 0;

                            while (contador < creditosList.Count && currentBatchSize < batchSize)
                            {
                                var mov = creditosList[contador];

                                var values = $@"
                                     (
                                        @valormercanciacliente{contador}, @cantidadcuotascliente{contador},
                                        (SELECT id FROM fenalpag WHERE numerodocumento = @fenalpagid{contador} LIMIT 1), 
                                        @tasainteres{contador}, @iva{contador}, @porcentajecuotainicial{contador},
                                        @numerocuotas{contador}, @valorlimitecredito{contador},
                                        (SELECT ""Id"" FROM ""FormasPago"" where ""Codigo"" = @idformapago{contador} LIMIT 1),
                                        @porcentajeaval{contador}, @numerocuotacobrainteres{contador},
                                        @valorminimocredito{contador}, @estado{contador}, @valorcuotainicialcliente{contador},
                                        @porcentajecuotainicialcliente{contador}, @periodicidad{contador}
                                     )";

                                valuesList.Add(values);
                                contador++;
                                currentBatchSize++;
                            }

                            // Combina todos los VALUES en una sola consulta
                            commandText.Append(string.Join(",", valuesList));

                            // Asigna el comando completo
                            cmd.CommandText = commandText.ToString();

                            // Agregar parámetros para el lote actual
                            for (int i = 0; i < currentBatchSize; i++)
                            {
                                var mpmovi = creditosList[contador - currentBatchSize + i];

                                // Convertir periodicidad
                                switch (mpmovi.periodicidad)
                                {
                                    case 1: xEstado = "Quincenal"; break;
                                    case 2: xEstado = "Mensual"; break;
                                    case 3: xEstado = "Semanal"; break;
                                    default: xEstado = "Desconocido"; break;
                                }

                                // Parametros
                                cmd.Parameters.AddWithValue($"@valormercanciacliente{contador - currentBatchSize + i}", mpmovi.valormercanciacliente);
                                cmd.Parameters.AddWithValue($"@cantidadcuotascliente{contador - currentBatchSize + i}", mpmovi.cantidadcuotascliente);
                                cmd.Parameters.AddWithValue($"@fenalpagid{contador - currentBatchSize + i}", mpmovi.fenalpagid.ToString());
                                cmd.Parameters.AddWithValue($"@tasainteres{contador - currentBatchSize + i}", mpmovi.tasainteres);
                                cmd.Parameters.AddWithValue($"@iva{contador - currentBatchSize + i}", mpmovi.iva);
                                cmd.Parameters.AddWithValue($"@porcentajecuotainicial{contador - currentBatchSize + i}", mpmovi.porcentajecuotainicial);
                                cmd.Parameters.AddWithValue($"@numerocuotas{contador - currentBatchSize + i}", mpmovi.numerocuotas);
                                cmd.Parameters.AddWithValue($"@valorlimitecredito{contador - currentBatchSize + i}", 0);
                                cmd.Parameters.AddWithValue($"@idformapago{contador - currentBatchSize + i}", mpmovi.idformapago?.ToString() ?? "");
                                cmd.Parameters.AddWithValue($"@porcentajeaval{contador - currentBatchSize + i}", mpmovi.porcentajeaval);
                                cmd.Parameters.AddWithValue($"@numerocuotacobrainteres{contador - currentBatchSize + i}", 0);
                                cmd.Parameters.AddWithValue($"@valorminimocredito{contador - currentBatchSize + i}", 0);
                                cmd.Parameters.AddWithValue($"@estado{contador - currentBatchSize + i}", true);
                                cmd.Parameters.AddWithValue($"@valorcuotainicialcliente{contador - currentBatchSize + i}", mpmovi.valorcuotainicialcliente);
                                cmd.Parameters.AddWithValue($"@porcentajecuotainicialcliente{contador - currentBatchSize + i}", mpmovi.porcentajecuotainicialcliente);
                                cmd.Parameters.AddWithValue($"@periodicidad{contador - currentBatchSize + i}", xEstado);
                            }
                            // Ejecutar el comando de inserción
                            cmd.ExecuteNonQuery();

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar Movimiento Mercapeso: {ex.InnerException}");
                        }
                        // Confirmar la transacción
                        tx.Commit();
                    }
                }

                conn.Close();
            }

            cronometer.Stop();
            Console.WriteLine("✔️ Proceso de inserción de créditos finalizado.");
            Console.WriteLine($"Tiempo total: {cronometer.Elapsed.TotalMinutes:F2} minutos");
        }

        //public static void InsertarCajasMovimientos(List<t_cabFact> movimientosList)
        //{
        //    int contador = 0;

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        foreach (var mov in movimientosList)
        //        {
        //            using (var tx = conn.BeginTransaction())
        //            using (var cmd = new NpgsqlCommand())
        //            {
        //                cmd.Connection = conn;
        //                cmd.Transaction = tx;

        //                try
        //                {
        //                    cmd.CommandText = @"
        //                INSERT INTO public.cajasmovimientos(
        //                    fechahoramovimiento, sucursalid, sucursalnombre, cajaid, cajacodigo, usuarioid, usuarionombre,
        //                    terceroid, nombretercero, tarjetamercapesosid, tarjetamercapesoscodigo, documentofactura,
        //                    totalventa, valorcambio, tipopagoid,numerofactura, documentoid,
        //                    documentonombre, identificacion)
        //                VALUES (
        //                    @fechahoramovimiento, 
        //                    (SELECT id FROM ""Sucursales"" WHERE codigo= @sucursalid LIMIT 1),
        //                    (SELECT nombre FROM ""Sucursales"" WHERE codigo= @sucursalid LIMIT 1), 
        //                    (
        //                    SELECT ""Id"" 
        //                    FROM ""CajaSucursal"" 
        //                    WHERE ""CodigoCaja"" = @cajacodigo 
        //                        AND ""IdSucursal"" = (SELECT id FROM ""Sucursales"" WHERE codigo = @sucursalid LIMIT 1)
        //                    LIMIT 1
        //                    ),
        //                    @cajacodigo,
        //                    (SELECT DISTINCT ""Id"" FROM ""AspNetUsers"" WHERE ""UserName"" = @usuarioid LIMIT 1),
        //                    @usuarionombre,
        //                    (SELECT DISTINCT ""Id"" FROM ""Terceros"" WHERE ""Identificacion"" = @terceroid LIMIT 1),
        //                    @nombretercero, 
        //                    (SELECT ""Id""	FROM ""MercaPesos"" where ""Tarjeta"" =@tarjetamercapesosid LIMIT 1),
        //                    (SELECT ""Id""	FROM ""MercaPesos"" where ""Codigo"" =@tarjetamercapesoscodigo LIMIT 1), @documentofactura,
        //                    @totalventa, @valorcambio, 
        //                    (SELECT id FROM tiposdepago where id = @tipopagoid LIMIT 1),
        //                    @numerofactura, 
        //                    (SELECT ""Id"" FROM ""Documentos"" WHERE ""Codigo"" = @documentoid LIMIT 1),
        //                    @documentonombre, @identificacion);";

        //                    cmd.Parameters.Clear();
        //                    cmd.Parameters.AddWithValue("@fechahoramovimiento", mov.fechahoramovimiento);
        //                    cmd.Parameters.AddWithValue("@sucursalid", mov.sucursalid);
        //                    //cmd.Parameters.AddWithValue("@sucursalnombre", mov.sucursalnombre);
        //                    //cmd.Parameters.AddWithValue("@cajaid", mov.cajaid);
        //                    cmd.Parameters.AddWithValue("@cajacodigo", mov.cajacodigo);
        //                    cmd.Parameters.AddWithValue("@usuarioid", mov.usuarioid);
        //                    cmd.Parameters.AddWithValue("@usuarionombre", mov.usuarionombre);
        //                    cmd.Parameters.AddWithValue("@terceroid", mov.terceroid);
        //                    cmd.Parameters.AddWithValue("@nombretercero", mov.nombretercero);
        //                    cmd.Parameters.AddWithValue("@tarjetamercapesosid", mov.tarjetamercapesosid == 0 ? DBNull.Value : mov.tarjetamercapesosid);
        //                    cmd.Parameters.AddWithValue("@tarjetamercapesoscodigo", string.IsNullOrWhiteSpace(mov.tarjetamercapesoscodigo) ? DBNull.Value : mov.tarjetamercapesoscodigo);
        //                    cmd.Parameters.AddWithValue("@documentofactura", mov.documentofactura ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue("@totalventa", mov.totalventa);
        //                    cmd.Parameters.AddWithValue("@valorcambio", mov.valorcambio);
        //                    cmd.Parameters.AddWithValue("@tipopagoid", mov.tipopagoid);
        //                    cmd.Parameters.AddWithValue("@numerofactura", mov.numerofactura);
        //                    cmd.Parameters.AddWithValue("@documentoid", mov.documentoid);
        //                    cmd.Parameters.AddWithValue("@documentonombre", mov.documentonombre);
        //                    cmd.Parameters.AddWithValue("@identificacion", mov.identificacion);

        //                    string finalQuery = GetInterpolatedQuery(cmd);
        //                    Console.WriteLine("SQL con valores:");
        //                    Console.WriteLine(finalQuery);

        //                    cmd.ExecuteNonQuery();
        //                    tx.Commit();

        //                    contador++;
        //                    Console.WriteLine($"✔ Registro insertado #{contador}");
        //                }
        //                catch (Exception ex)
        //                {
        //                    tx.Rollback();
        //                    Console.WriteLine($"⚠️ Error en registro #{contador + 1}: {ex.Message}");
        //                }
        //            }
        //        }

        //        Console.WriteLine("✅ Inserción finalizada.");
        //    }
        //}

        ///---- pruebas

        //public static void InsertarCajasMovimientos(List<t_cabFact> movimientosList, int batchSize = 100)
        //public static async Task InsertarCajasMovimientos(List<t_cabFact> movimientosList, int batchSize = 100)
        //{
        //    try
        //    {
        //        int contador = 0;
        //        var cronometer = Stopwatch.StartNew();
        //        Stopwatch sw = new Stopwatch();
        //        sw.Start();


        //        //using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Timeout=180;CommandTimeout=180;"))
        //        //using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Timeout=180;"))
        //        using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
        //        {
        //            if (conn.State != System.Data.ConnectionState.Open)
        //                await conn.OpenAsync();

        //            while (contador < movimientosList.Count)
        //            {
        //                using (var tx = await conn.BeginTransactionAsync())
        //                using (var cmd = new NpgsqlCommand())
        //                {
        //                    cmd.Connection = conn;
        //                    cmd.Transaction = tx;

        //                    var commandText = new StringBuilder();





        //                    commandText.AppendLine(@"
        //                INSERT INTO public.cajasmovimientos(
        //                    fechahoramovimiento, sucursalid, sucursalnombre, cajaid,cajacodigo, usuarioid, usuarionombre,
        //                    terceroid, nombretercero, tarjetamercapesosid, tarjetamercapesoscodigo, documentofactura,
        //                    totalventa, valorcambio, tipopagoid,numerofactura, documentoid,
        //                    documentonombre, identificacion)VALUES");

        //                    var valuesList = new List<string>();
        //                    int currentBatchSize = 0;

        //                    while (contador < movimientosList.Count && currentBatchSize < batchSize)
        //                    {
        //                        var mov = movimientosList[contador];


        //                        var values = $@"
        //            (
        //                    @fechahoramovimiento{contador}, 
        //                    (SELECT id FROM ""Sucursales"" WHERE codigo= @sucursalid{contador} LIMIT 1),
        //                    (SELECT nombre FROM ""Sucursales"" WHERE codigo= @sucursalid{contador} LIMIT 1), 
        //                    (
        //                    SELECT ""Id"" 
        //                    FROM ""CajaSucursal"" 
        //                    WHERE ""CodigoCaja"" = @cajacodigo{contador} 
        //                        AND ""IdSucursal"" = (SELECT id FROM ""Sucursales"" WHERE codigo = @sucursalid{contador} LIMIT 1)
        //                    LIMIT 1
        //                    ),
        //                    @cajacodigo{contador},
        //                    (SELECT DISTINCT ""Id"" FROM ""AspNetUsers"" WHERE ""UserName"" = @usuarioid{contador} LIMIT 1),
        //                    @usuarionombre{contador},
        //                    (SELECT DISTINCT ""Id"" FROM ""Terceros"" WHERE ""Identificacion"" = @terceroid{contador} LIMIT 1),
        //                    @nombretercero{contador}, 
        //                    (SELECT ""Id""	FROM ""MercaPesos"" where ""Tarjeta"" =@tarjetamercapesosid{contador} LIMIT 1),
        //                    (SELECT ""Id""	FROM ""MercaPesos"" where ""Codigo"" =@tarjetamercapesoscodigo{contador} LIMIT 1), @documentofactura{contador},
        //                    @totalventa{contador}, @valorcambio{contador}, 
        //                    (SELECT id FROM tiposdepago where id = @tipopagoid{contador} LIMIT 1),
        //                    @numerofactura{contador}, 
        //                    (SELECT ""Id"" FROM ""Documentos"" WHERE ""Codigo"" = @documentoid{contador} LIMIT 1),
        //                    @documentonombre{contador}, @identificacion{contador}
        //                 )";

        //                        valuesList.Add(values);
        //                        contador++;
        //                        currentBatchSize++;
        //                    }

        //                    // Combina todos los VALUES en una sola consulta
        //                    commandText.Append(string.Join(",", valuesList));

        //                    // Asigna el comando completo
        //                    cmd.CommandText = commandText.ToString();

        //                    // Agregar parámetros para el lote actual
        //                    for (int i = 0; i < currentBatchSize; i++)
        //                    {

        //                        var mov = movimientosList[contador - currentBatchSize + i];

        //                        cmd.Parameters.AddWithValue($"@fechahoramovimiento{contador - currentBatchSize + i}", mov.fechahoramovimiento);
        //                        cmd.Parameters.AddWithValue($"@sucursalid{contador - currentBatchSize + i}", mov.sucursalid);
        //                        cmd.Parameters.AddWithValue($"@cajacodigo{contador - currentBatchSize + i}", mov.cajacodigo);
        //                        cmd.Parameters.AddWithValue($"@usuarioid{contador - currentBatchSize + i}", mov.usuarioid);
        //                        cmd.Parameters.AddWithValue($"@usuarionombre{contador - currentBatchSize + i}", mov.usuarionombre);
        //                        cmd.Parameters.AddWithValue($"@terceroid{contador - currentBatchSize + i}", mov.terceroid);
        //                        cmd.Parameters.AddWithValue($"@nombretercero{contador - currentBatchSize + i}", mov.nombretercero);
        //                        cmd.Parameters.AddWithValue($"@tarjetamercapesosid{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(mov.tarjetamercapesoscodigo) ? DBNull.Value : mov.tarjetamercapesoscodigo);
        //                        cmd.Parameters.AddWithValue($"@tarjetamercapesoscodigo{contador - currentBatchSize + i}", mov.tarjetamercapesosid == 0 ? DBNull.Value : mov.tarjetamercapesosid);
        //                        cmd.Parameters.AddWithValue($"@documentofactura{contador - currentBatchSize + i}", mov.documentofactura ?? (object)DBNull.Value);
        //                        cmd.Parameters.AddWithValue($"@totalventa{contador - currentBatchSize + i}", mov.totalventa);
        //                        cmd.Parameters.AddWithValue($"@valorcambio{contador - currentBatchSize + i}", mov.valorcambio);
        //                        cmd.Parameters.AddWithValue($"@tipopagoid{contador - currentBatchSize + i}", mov.tipopagoid);
        //                        cmd.Parameters.AddWithValue($"@numerofactura{contador - currentBatchSize + i}", mov.numerofactura);
        //                        cmd.Parameters.AddWithValue($"@documentoid{contador - currentBatchSize + i}", mov.documentoid);
        //                        cmd.Parameters.AddWithValue($"@documentonombre{contador - currentBatchSize + i}", mov.documentonombre);
        //                        cmd.Parameters.AddWithValue($"@identificacion{contador - currentBatchSize + i}", mov.identificacion);



        //                    }

        //                    Console.WriteLine(cmd.CommandText);

        //                    //Console.WriteLine("Parámetros:");
        //                    //foreach (NpgsqlParameter param in cmd.Parameters)
        //                    //{
        //                    //    Console.WriteLine($"{param.ParameterName}: {param.Value}");
        //                    //}

        //                    //cmd.ExecuteNonQuery();
        //                    await cmd.ExecuteNonQueryAsync();
        //                    await tx.CommitAsync();
        //                }
        //            }
        //        }
        //        cronometer.Stop();
        //        TimeSpan tiempoTranscurrido = cronometer.Elapsed;
        //        Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
        //        Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
        //        Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");

        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex.InnerException.Message);

        //    }

        //}

        public static async Task InsertarCajasMovimientos(List<t_cabFact> movimientosList, int batchSize = 600)
        {
            try
            {
                // Validación inicial
                if (movimientosList == null || !movimientosList.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {

                   

                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();



                    // se crean la variables de memoria 
                    var sucursales           = new Dictionary<string, (int Id, string Nombre)>();
                    var usuarios             = new Dictionary<string, (string Id, string Nombre)>();
                    var terceros             = new Dictionary<long, int>();
                    var mercaPesos           = new Dictionary<long, int>();
                    var cajaSucursalDict = new Dictionary<string, int>();
                    //var mercaPesosPorCodigo  = new Dictionary<string, int>();
                    Dictionary<int, int> mercaPesosPorCodigo = new();


                    using (var cmd = new NpgsqlCommand(@"
                        SELECT c.""CodigoCaja"", s.""codigo"" AS CodigoSucursal, c.""Id""
                        FROM public.""CajaSucursal"" c
                        JOIN ""Sucursales"" s ON s.id = c.""IdSucursal""
                    ", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigoCaja = reader.GetString(0);
                            string codigoSucursal = reader.GetString(1);
                            int id = reader.GetInt32(2);

                            string key = $"{codigoCaja}-{codigoSucursal}";
                            cajaSucursalDict[key] = id;
                        }
                    }



                    using (var cmd = new NpgsqlCommand(@"SELECT codigo, id, nombre FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);
                            string nombre = reader.GetString(2);
                            sucursales[codigo] = (id, nombre);
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""UserName"", ""Id"",""NombreCompleto"" FROM ""AspNetUsers""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string username = reader.GetString(0);
                            string id = reader.GetString(1);
                            string nombre = reader.GetString(2);
                            usuarios[username] = (id, nombre);
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Identificacion"", ""Id"" FROM ""Terceros""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            long identificacion = reader.GetInt64(0);
                            int id = reader.GetInt32(1);
                            terceros[identificacion] = id;
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM ""MercaPesos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);       // "Id"
                            long codigo = reader.GetInt64(1); // "Codigo" (ajusta tipo real, si no es string cámbialo)

                            mercaPesos[codigo] = id;


                        }
                    }


                    while (contador < movimientosList.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                            INSERT INTO public.cajasmovimientos(
                                fechahoramovimiento, sucursalid, sucursalnombre, cajaid,cajacodigo, usuarioid, usuarionombre,
                                terceroid, nombretercero, tarjetamercapesosid, tarjetamercapesoscodigo, documentofactura,
                                totalventa, valorcambio, tipopagoid,numerofactura, documentoid,
                                documentonombre, identificacion)VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < movimientosList.Count && currentBatchSize < batchSize)
                                {
                                    var mov = movimientosList[contador];

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"
                                (
                                    @fechahoramovimiento{contador}, 
                                    @sucursalid{contador} ,
                                    @sucursalnombre{contador} ,
                                    @idcaja{contador},
                                    @cajacodigo{contador},
                                    @usuarioid{contador} ,
                                    @usuarionombre{contador},
                                    @terceroid{contador} ,
                                    @nombretercero{contador}, 
                                    @tarjetamercapesosid{contador} ,
                                    @tarjetamercapesoscodigo{contador} , @documentofactura{contador},
                                    @totalventa{contador}, @valorcambio{contador}, 
                                    @tipopagoid{contador},
                                    @numerofactura{contador}, 
                                    @documentoid{contador},
                                    @documentonombre{contador}, @identificacion{contador}
                                )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var mov = movimientosList[batchStartIndex + i];

                                    // Validación adicional
                                    if (mov == null) continue;

                                    cmd.Parameters.AddWithValue($"@sucursalid{batchStartIndex + i}",
                                    sucursales.ContainsKey(mov.sucursalid) ? sucursales[mov.sucursalid].Id : (object)DBNull.Value);


                                    cmd.Parameters.AddWithValue($"@fechahoramovimiento{batchStartIndex + i}", mov.fechahoramovimiento);
                                    //cmd.Parameters.AddWithValue($"@sucursalid{batchStartIndex + i}", mov.sucursalid ?? (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@sucursalid{batchStartIndex + i}",
                                    sucursales.ContainsKey(mov.sucursalid) ? sucursales[mov.sucursalid].Id : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@sucursalnombre{batchStartIndex + i}",
                                    sucursales.ContainsKey(mov.sucursalid) ? sucursales[mov.sucursalid].Nombre : (object)DBNull.Value);

                                    //cmd.Parameters.AddWithValue($"@idcaja{batchStartIndex + i}", Convert.ToInt32(mov.idcaja));

                                    string key = $"{mov.cajacodigo}-{mov.sucursalid}";

                                    cmd.Parameters.AddWithValue(
                                        $"@idcaja{batchStartIndex + i}",
                                        cajaSucursalDict.TryGetValue(key, out var idX)
                                            ? (object)idX
                                            : DBNull.Value
                                    );

                                    cmd.Parameters.AddWithValue($"@cajacodigo{batchStartIndex + i}", mov.cajacodigo ?? (object)DBNull.Value);
                                    //cmd.Parameters.AddWithValue($"@usuarioid{batchStartIndex + i}", mov.usuarioid.Trim() ?? (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@usuarioid{batchStartIndex + i}",
                                    usuarios.ContainsKey(mov.usuarioid) ? usuarios[mov.usuarioid].Id : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@usuarionombre{batchStartIndex + i}",
                                    usuarios.ContainsKey(mov.usuarioid) ? usuarios[mov.usuarioid].Nombre : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@usuarionombre{batchStartIndex + i}", mov.usuarionombre ?? (object)DBNull.Value);
                                    //cmd.Parameters.AddWithValue($"@terceroid{batchStartIndex + i}", mov.terceroid);
                                    cmd.Parameters.AddWithValue(
                                        $"@terceroid{batchStartIndex + i}",
                                       (mov.identificacion.HasValue && terceros.TryGetValue(mov.identificacion.Value, out var idT))
                                           ? (object)idT
                                           : DBNull.Value
                                   );

                                    cmd.Parameters.AddWithValue($"@nombretercero{batchStartIndex + i}", mov.nombretercero ?? (object)DBNull.Value);
                                    //cmd.Parameters.AddWithValue($"@tarjetamercapesosid{batchStartIndex + i}", string.IsNullOrWhiteSpace(mov.tarjetamercapesoscodigo) ? DBNull.Value : mov.tarjetamercapesoscodigo);
                                    //cmd.Parameters.AddWithValue($"@tarjetamercapesoscodigo{batchStartIndex + i}", mov.tarjetamercapesosid == 0 ? DBNull.Value : mov.tarjetamercapesosid);

                                    cmd.Parameters.AddWithValue(
                                       $"@tarjetamercapesosid{batchStartIndex + i}",
                                      (mov.tarjetamercapesosid.HasValue && mercaPesos.TryGetValue(mov.tarjetamercapesosid.Value, out var ids))
                                          ? (object)ids
                                          : DBNull.Value
                                  );

                                    cmd.Parameters.AddWithValue($"@tarjetamercapesoscodigo{batchStartIndex + i}", mov.tarjetamercapesoscodigo);
                                    cmd.Parameters.AddWithValue($"@documentofactura{batchStartIndex + i}", mov.documentofactura ?? (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@totalventa{batchStartIndex + i}", mov.totalventa);
                                    cmd.Parameters.AddWithValue($"@valorcambio{batchStartIndex + i}", mov.valorcambio);
                                    cmd.Parameters.AddWithValue($"@tipopagoid{batchStartIndex + i}", Convert.ToInt32(mov.tipopagoid));
                                    cmd.Parameters.AddWithValue($"@numerofactura{batchStartIndex + i}", mov.numerofactura);
                                    cmd.Parameters.AddWithValue($"@documentoid{batchStartIndex + i}", Convert.ToInt32(mov.documentoid));
                                    cmd.Parameters.AddWithValue($"@documentonombre{batchStartIndex + i}", mov.documentonombre ?? (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@identificacion{batchStartIndex + i}", mov.identificacion);
                                }

                                Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                Console.WriteLine($"Lote completado. Registros procesados: {contador}/{movimientosList.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
           }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        //public static void InsertarCajasDetalles(List<t_detFact> detfactList)
        //{
        //    int contador = 0;

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        foreach (var detalle in detfactList)
        //        {
        //            using (var tx = conn.BeginTransaction())
        //            using (var cmd = new NpgsqlCommand())
        //            {
        //                cmd.Connection = conn;
        //                cmd.Transaction = tx;

        //                try
        //                {
        //                    cmd.CommandText = @"
        //                    INSERT INTO public.cajasmovimientosgenerales(
        //                        cajasmovimientosid, productoid, productonombre, productovalorsiniva,
        //                        productoporcentajeiva, productovaloriva, productoexento,
        //                        promocionid, promocionnombre, promocionvalordscto, productovalorimptoconsumo,
        //                        vendedorid, valordescuentogeneral, totalitem, productoidmarca, productoidlinea,
        //                        productoidsublinea, valoracumulartarejetamercapesos, cantidaddelproducto,
        //                        productovalorantesdscto, productoporcentajedscto, productovalordescuento,
        //                        factorimptoconsumo, productonombremarca, productonombrelinea,
        //                        productonombresublinea)
        //                    VALUES (

        //                        (SELECT id	FROM public.cajasmovimientos where numerofactura = @cajasmovimientosid LIMIT 1),
        //                        (SELECT ""Id"" FROM public.""Productos"" where ""Codigo"" = @productoid LIMIT 1),
        //                        @productonombre, @productovalorsiniva,
        //                        @productoporcentajeiva, @productovaloriva, @productoexento,
        //                        (SELECT id FROM public.""PromocionesDescuentos"" where codigo =@promocionid LIMIT 1),
        //                        @promocionnombre, @promocionvalordscto, @productovalorimptoconsumo,
        //                        (SELECT id FROM public.vendedores where codigo =@vendedorid LIMIT 1),
        //                        @valordescuentogeneral, @totalitem,
        //                        (SELECT id FROM public.""MarcasProductos"" where codigo =  @productoidmarca LIMIT 1),
        //                        (SELECT id FROM public.""LineasProductos"" where codigo = @productoidlinea LIMIT 1),
        //                        (SELECT id FROM public.""SublineasProductos"" where codigo =@productoidsublinea LIMIT 1),
        //                        @valoracumulartarejetamercapesos, @cantidaddelproducto,
        //                        @productovalorantesdscto, @productoporcentajedscto, @productovalordescuento,
        //                        @factorimptoconsumo, 
        //                        (SELECT nombre FROM public.""MarcasProductos"" where codigo =  @productoidmarca LIMIT 1),
        //                        (SELECT nombre FROM public.""LineasProductos"" where codigo = @productoidlinea LIMIT 1),
        //                        (SELECT nombre FROM public.""SublineasProductos"" where codigo =@productoidsublinea LIMIT 1)
        //                        );";

        //                    cmd.Parameters.Clear();
        //                    cmd.Parameters.AddWithValue("@cajasmovimientosid", Convert.ToString(detalle.cajasmovimientosid));
        //                    cmd.Parameters.AddWithValue("@productoid", detalle.productoid);
        //                    cmd.Parameters.AddWithValue("@productonombre", detalle.productonombre);
        //                    cmd.Parameters.AddWithValue("@productovalorsiniva", detalle.productovalorsiniva);
        //                    cmd.Parameters.AddWithValue("@productoporcentajeiva", detalle.productoporcentajeiva);
        //                    cmd.Parameters.AddWithValue("@productovaloriva", detalle.productovaloriva);
        //                    cmd.Parameters.AddWithValue("@productoexento", detalle.productoexento == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@promocionid", detalle.promocionid);
        //                    cmd.Parameters.AddWithValue("@promocionnombre", detalle.promocionnombre ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue("@promocionvalordscto", detalle.promocionvalordscto);
        //                    cmd.Parameters.AddWithValue("@productovalorimptoconsumo", detalle.productovalorimptoconsumo);
        //                    cmd.Parameters.AddWithValue("@vendedorid", detalle.vendedorid ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue("@valordescuentogeneral", detalle.valordescuentogeneral);
        //                    cmd.Parameters.AddWithValue("@totalitem", detalle.totalitem);
        //                    cmd.Parameters.AddWithValue("@productoidmarca", detalle.productoidmarca);
        //                    cmd.Parameters.AddWithValue("@productoidlinea", detalle.productoidlinea);
        //                    cmd.Parameters.AddWithValue("@productoidsublinea", detalle.productoidsublinea);
        //                    cmd.Parameters.AddWithValue("@valoracumulartarejetamercapesos", detalle.valoracumulartarejetamercapesos);
        //                    cmd.Parameters.AddWithValue("@cantidaddelproducto", detalle.cantidaddelproducto);
        //                    cmd.Parameters.AddWithValue("@productovalorantesdscto", detalle.productovalorantesdscto);
        //                    cmd.Parameters.AddWithValue("@productoporcentajedscto", detalle.productoporcentajedscto);
        //                    cmd.Parameters.AddWithValue("@productovalordescuento", detalle.productovalordescuento);
        //                    cmd.Parameters.AddWithValue("@factorimptoconsumo", detalle.factorimptoconsumo);
        //                    cmd.Parameters.AddWithValue("@productonombremarca", detalle.productonombremarca ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue("@productonombrelinea", detalle.productonombrelinea ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue("@productonombresublinea", detalle.productonombresublinea ?? (object)DBNull.Value);


        //                    string finalQuery = GetInterpolatedQuery(cmd);
        //                    Console.WriteLine("SQL con valores:");
        //                    Console.WriteLine(finalQuery);

        //                    cmd.ExecuteNonQuery();
        //                    tx.Commit();

        //                    contador++;
        //                    Console.WriteLine($"✔ Registro insertado #{contador}");
        //                }
        //                catch (Exception ex)
        //                {
        //                    //tx.Rollback();
        //                    Console.WriteLine($"⚠️ Error en registro #{contador + 1}: {ex.Message}");
        //                }
        //            }
        //        }

        //        Console.WriteLine("✅ Inserción finalizada.");
        //    }
        //}

        public static async Task InsertarCajasDetalles(List<t_detFact> detfactList, int batchSize = 100)
        {
            try
            {
                // Validación inicial
                if (detfactList == null || !detfactList.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Include Error Detail=true"))
                {



                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();





                    // se crean la variables de memoria 

                    // Diccionarios por tabla
                    var cajasMovimientos = new Dictionary<(string numerofactura, string documentofactura), int>();     // numerofactura → id
                    var productos = new Dictionary<string, int>();     // codigo → Id
                    var promociones = new Dictionary<string, int>();     // codigo → Id
                    var vendedores = new Dictionary<string, int>();     // codigo → Id
                    var marcasProductos = new Dictionary<string, int>();     // codigo → Id
                    var lineasProductos = new Dictionary<string, int>();     // codigo → Id
                    var sublineasProductos = new Dictionary<string, int>();     // codigo → Id

                    // Diccionarios para los nombres
                    var marcasNombres = new Dictionary<string, string>();  // codigo → nombre
                    var lineasNombres = new Dictionary<string, string>();  // codigo → nombre
                    var sublineasNombres = new Dictionary<string, string>();  // codigo → nombre
                    


                    // Cargar diccionario de CajasMovimientos
                    using (var cmd = new NpgsqlCommand(@"SELECT id, numerofactura,documentofactura FROM public.cajasmovimientos", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string numeroFactura = reader.GetString(1);
                            string documentofactura = reader.GetString(2);

                            cajasMovimientos[(numeroFactura,documentofactura)] = id;
                        }
                    }

                    // Cargar diccionario de Productos
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM public.""Productos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);

                            productos[codigo] = id;
                        }
                    }

                    // Cargar diccionario de Promociones
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM public.""PromocionesDescuentos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);

                            promociones[codigo] = id;
                        }
                    }

                    // Cargar diccionario de Vendedores
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM public.vendedores", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);          // id
                            string codigo = reader.GetString(1);  // codigo (string en la tabla)

                            vendedores[codigo] = id;              // asignación correcta
                        }
                    }

                    // Cargar diccionario de Marcas
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo, nombre FROM public.""MarcasProductos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            string nombre = reader.IsDBNull(2) ? null : reader.GetString(2);

                            marcasProductos[codigo] = id;
                            if (nombre != null)
                                marcasNombres[codigo] = nombre;
                        }
                    }

                    // Cargar diccionario de Líneas
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo, nombre FROM public.""LineasProductos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            string nombre = reader.IsDBNull(2) ? null : reader.GetString(2);

                            lineasProductos[codigo] = id;
                            if (nombre != null)
                                lineasNombres[codigo] = nombre;
                        }
                    }

                    // Cargar diccionario de Sublíneas
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo, nombre FROM public.""SublineasProductos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            string nombre = reader.IsDBNull(2) ? null : reader.GetString(2);

                            sublineasProductos[codigo] = id;
                            if (nombre != null)
                                sublineasNombres[codigo] = nombre;
                        }
                    }


                    while (contador < detfactList.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                     INSERT INTO public.cajasmovimientosgenerales(
                     cajasmovimientosid, productoid, productonombre, productovalorsiniva,
                     productoporcentajeiva, productovaloriva, productoexento,
                     promocionid, promocionnombre, promocionvalordscto, productovalorimptoconsumo,
                     vendedorid, valordescuentogeneral, totalitem, productoidmarca, productoidlinea,
                     productoidsublinea, valoracumulartarejetamercapesos, cantidaddelproducto,
                     productovalorantesdscto, productoporcentajedscto, productovalordescuento,
                     factorimptoconsumo, productonombremarca, productonombrelinea,
                     productonombresublinea)VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < detfactList.Count && currentBatchSize < batchSize)
                                {
                                    var mov = detfactList[contador];

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"
                         (
                            @cajasmovimientosid{contador},
                            @productoid{contador},
                            @productonombre{contador}, @productovalorsiniva{contador},
                            @productoporcentajeiva{contador}, @productovaloriva{contador}, @productoexento{contador},
                            @promocionid{contador},
                            @promocionnombre{contador}, @promocionvalordscto{contador}, @productovalorimptoconsumo{contador},
                            @vendedorid{contador},
                            @valordescuentogeneral{contador}, @totalitem{contador},
                            @productoidmarca{contador},
                            @productoidlinea{contador},
                            @productoidsublinea{contador},
                            @valoracumulartarejetamercapesos{contador}, @cantidaddelproducto{contador},
                            @productovalorantesdscto{contador}, @productoporcentajedscto{contador}, @productovalordescuento{contador},
                            @factorimptoconsumo{contador}, 
                            @nombremarca{contador},
                            @nombrelinea{contador},
                            @nombresublinea{contador}
                         )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var detalle = detfactList[batchStartIndex + i];

                                    // Validación adicional
                                    if (detalle == null) continue;

                                    var clave = (detalle.cajasmovimientosid.ToString(), detalle.documentofactura);

                                    if (cajasMovimientos.TryGetValue(clave, out int idCajaMovimiento))
                                    {
                                        cmd.Parameters.AddWithValue(
                                            $"@cajasmovimientosid{batchStartIndex + i}",
                                            idCajaMovimiento
                                        );
                                    }
                                    else
                                    {
                                        cmd.Parameters.AddWithValue(
                                            $"@cajasmovimientosid{batchStartIndex + i}",
                                            DBNull.Value
                                        );
                                    }

                                    cmd.Parameters.AddWithValue(
                                        $"@productoid{batchStartIndex + i}",
                                        productos.ContainsKey(detalle.productoid)
                                            ? productos[detalle.productoid]
                                            : (object)DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue($"@productonombre{batchStartIndex + i}", detalle.productonombre);
                                    cmd.Parameters.AddWithValue($"@productovalorsiniva{batchStartIndex + i}", detalle.productovalorsiniva);
                                    cmd.Parameters.AddWithValue($"@productoporcentajeiva{batchStartIndex + i}", detalle.productoporcentajeiva);
                                    cmd.Parameters.AddWithValue($"@productovaloriva{batchStartIndex + i}", detalle.productovaloriva);
                                    cmd.Parameters.AddWithValue($"@productoexento{batchStartIndex + i}", detalle.productoexento == 1 ? true : false);
                                    cmd.Parameters.AddWithValue(
                                        $"@promocionid{batchStartIndex + i}",
                                        promociones.ContainsKey(detalle.promocionid)
                                            ? promociones[detalle.promocionid]
                                            : (object)DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue($"@promocionnombre{batchStartIndex + i}", detalle.promocionnombre ?? (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@promocionvalordscto{batchStartIndex + i}", detalle.promocionvalordscto);
                                    cmd.Parameters.AddWithValue($"@productovalorimptoconsumo{batchStartIndex + i}", detalle.productovalorimptoconsumo);
                                    cmd.Parameters.AddWithValue(
                                        $"@vendedorid{batchStartIndex + i}",
                                        vendedores.ContainsKey(detalle.vendedorid)
                                            ? vendedores[detalle.vendedorid]
                                            : (object)DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue($"@valordescuentogeneral{batchStartIndex + i}", detalle.valordescuentogeneral);
                                    cmd.Parameters.AddWithValue($"@totalitem{batchStartIndex + i}", detalle.totalitem);
                                    cmd.Parameters.AddWithValue(
                                        $"@productoidmarca{batchStartIndex + i}",
                                        marcasProductos.ContainsKey(detalle.productoidmarca)
                                            ? marcasProductos[detalle.productoidmarca]
                                            : (object)DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@productoidlinea{batchStartIndex + i}",
                                        lineasProductos.ContainsKey(detalle.productoidlinea)
                                            ? lineasProductos[detalle.productoidlinea]
                                            : (object)DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@productoidsublinea{batchStartIndex + i}",
                                        sublineasProductos.ContainsKey(detalle.productoidsublinea)
                                            ? sublineasProductos[detalle.productoidsublinea]
                                            : (object)DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue($"@valoracumulartarejetamercapesos{batchStartIndex + i}", detalle.valoracumulartarejetamercapesos);
                                    cmd.Parameters.AddWithValue($"@cantidaddelproducto{batchStartIndex + i}", detalle.cantidaddelproducto);
                                    cmd.Parameters.AddWithValue($"@productovalorantesdscto{batchStartIndex + i}", detalle.productovalorantesdscto);
                                    cmd.Parameters.AddWithValue($"@productoporcentajedscto{batchStartIndex + i}", detalle.productoporcentajedscto);
                                    cmd.Parameters.AddWithValue($"@productovalordescuento{batchStartIndex + i}", detalle.productovalordescuento);
                                    cmd.Parameters.AddWithValue($"@factorimptoconsumo{batchStartIndex + i}", detalle.factorimptoconsumo);
                                    cmd.Parameters.AddWithValue(
                                        $"@nombremarca{batchStartIndex + i}",
                                        marcasNombres.ContainsKey(detalle.productoidmarca)
                                            ? marcasNombres[detalle.productoidmarca]
                                            : (object)DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@nombrelinea{batchStartIndex + i}",
                                        lineasNombres.ContainsKey(detalle.productoidlinea)
                                            ? lineasNombres[detalle.productoidlinea]
                                            : (object)DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@nombresublinea{batchStartIndex + i}",
                                        sublineasNombres.ContainsKey(detalle.productoidsublinea)
                                            ? sublineasNombres[detalle.productoidsublinea]
                                            : (object)DBNull.Value
                                    );
                                }

                                Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                Console.WriteLine($"Lote completado. Registros procesados: {contador}/{detfactList.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
            }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        //public static void insertarFpago(List<t_fpago> fpago_list, int batchSize = 100)
        //{
        //    int contador = 0;
        //    var cronometer = Stopwatch.StartNew();
        //    Stopwatch sw = new Stopwatch();
        //    sw.Start();

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        while (contador < fpago_list.Count)
        //        {
        //            using (var tx = conn.BeginTransaction())
        //            using (var cmd = new NpgsqlCommand())
        //            {
        //                cmd.Connection = conn;
        //                cmd.Transaction = tx;

        //                var commandText = new StringBuilder();

        //                commandText.AppendLine(@"
        //                INSERT INTO cajasmovimientosformasdepago (
        //                cajasmovimientosid, formadepagoid, valor, nombreformapago, codigodatafono,
        //                redencion)VALUES");

        //                var valuesList = new List<string>();
        //                int currentBatchSize = 0;

        //                while (contador < fpago_list.Count && currentBatchSize < batchSize)
        //                {
        //                    var fpago = fpago_list[contador];

        //                    var values = $@"



        //            (
        //                (SELECT id	FROM public.cajasmovimientos where numerofactura =@numero{contador} LIMIT 1),
        //                (SELECT DISTINCT ""Id"" FROM ""FormasPago"" WHERE ""Codigo"" = @formadepagoId{contador} LIMIT 1),
        //                @valor{contador}, @nombreFpago{contador}, @codigodatafono{contador}, @redencion{contador}
        //                 )";

        //                    valuesList.Add(values);
        //                    contador++;
        //                    currentBatchSize++;
        //                }

        //                // Combina todos los VALUES en una sola consulta
        //                commandText.Append(string.Join(",", valuesList));

        //                // Asigna el comando completo
        //                cmd.CommandText = commandText.ToString();

        //                // Agregar parámetros para el lote actual
        //                for (int i = 0; i < currentBatchSize; i++)
        //                {
        //                    var fpago = fpago_list[contador - currentBatchSize + i];



        //                        Console.WriteLine("posicion.." +  i );


        //                    cmd.Parameters.AddWithValue($"@numero{contador - currentBatchSize + i}", fpago.numero.ToString());
        //                    //cmd.Parameters.AddWithValue("@cajasmovimientosid{contador - currentBatchSize + i}", fpago.cajasmovimientosid);
        //                    cmd.Parameters.AddWithValue($"@formadepagoId{contador - currentBatchSize + i}", fpago.formadepagoId);
        //                    cmd.Parameters.AddWithValue($"@valor{contador - currentBatchSize + i}", fpago.valor);
        //                    cmd.Parameters.AddWithValue($"@nombreFpago{contador - currentBatchSize + i}", fpago.nombreFpago);
        //                    cmd.Parameters.AddWithValue($"@codigodatafono{contador - currentBatchSize + i}", fpago.codigoDatafon);
        //                    cmd.Parameters.AddWithValue($"@redencion{contador - currentBatchSize + i}", fpago.redencion);

        //                }

        //                cmd.ExecuteNonQuery();
        //                tx.Commit();
        //            }
        //        }
        //    }
        //    cronometer.Stop();
        //    TimeSpan tiempoTranscurrido = cronometer.Elapsed;
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
        //    Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
        //}


        //public static void insertarFpago(List<t_fpago> fpago_list, int batchSize = 100)
        //{
        //    int contador = 0;
        //    int registrosInsertados = 0;
        //    int registrosOmitidos = 0;
        //    var cronometer = Stopwatch.StartNew();

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {

        //        conn.Open();

        //        while (contador < fpago_list.Count)
        //        {
        //            using (var tx = conn.BeginTransaction())
        //            using (var cmd = new NpgsqlCommand())
        //            {
        //                cmd.Connection = conn;
        //                cmd.Transaction = tx;

        //                var commandText = new StringBuilder();
        //                commandText.AppendLine(@"
        //        INSERT INTO cajasmovimientosformasdepago (
        //        cajasmovimientosid, formadepagoid, valor, nombreformapago, codigodatafono,
        //        redencion)VALUES");

        //                var valuesList = new List<string>();
        //                var registrosValidosEnLote = new List<t_fpago>();
        //                int currentBatchSize = 0;
        //                var cajasMovimientos = new Dictionary<string, int>();
        //                var formasPago = new Dictionary<string, int>();


        //                while (contador < fpago_list.Count && currentBatchSize < batchSize)
        //                {
        //                    var fpago = fpago_list[contador];

        //                    // Verificar si existe el registro en cajasmovimientos
        //                    using (var checkCmd = new NpgsqlCommand(
        //                        "SELECT COUNT(*) FROM public.cajasmovimientos WHERE numerofactura = @numero",
        //                        conn, tx))
        //                    {
        //                        checkCmd.Parameters.AddWithValue("@numero", fpago.numero.ToString());
        //                        var existe = Convert.ToInt32(checkCmd.ExecuteScalar()) > 0;

        //                        if (existe)
        //                        {
        //                            registrosValidosEnLote.Add(fpago);

        //                            var values = $@"
        //                    (
        //                        (SELECT id FROM public.cajasmovimientos where numerofactura = @numero{registrosValidosEnLote.Count - 1} LIMIT 1),
        //                        (SELECT DISTINCT ""Id"" FROM ""FormasPago"" WHERE ""Codigo"" = @formadepagoId{registrosValidosEnLote.Count - 1} LIMIT 1),
        //                        @valor{registrosValidosEnLote.Count - 1}, @nombreFpago{registrosValidosEnLote.Count - 1}, 
        //                        @codigodatafono{registrosValidosEnLote.Count - 1}, @redencion{registrosValidosEnLote.Count - 1}
        //                    )";

        //                            valuesList.Add(values);
        //                        }
        //                        else
        //                        {
        //                            Console.WriteLine($"⚠ Omitiendo registro: Número {fpago.numero} no existe en cajasmovimientos");
        //                            registrosOmitidos++;
        //                        }
        //                    }

        //                    contador++;
        //                    currentBatchSize++;
        //                }

        //                // Solo ejecutar si hay registros válidos
        //                if (registrosValidosEnLote.Count > 0)
        //                {
        //                    commandText.Append(string.Join(",", valuesList));
        //                    cmd.CommandText = commandText.ToString();

        //                    // Agregar parámetros
        //                    for (int i = 0; i < registrosValidosEnLote.Count; i++)
        //                    {
        //                        var fpago = registrosValidosEnLote[i];

        //                        cmd.Parameters.AddWithValue($"@numero{i}", fpago.numero.ToString());
        //                        cmd.Parameters.AddWithValue($"@formadepagoId{i}", fpago.formadepagoId);
        //                        cmd.Parameters.AddWithValue($"@valor{i}", fpago.valor);
        //                        cmd.Parameters.AddWithValue($"@nombreFpago{i}", fpago.nombreFpago);
        //                        cmd.Parameters.AddWithValue($"@codigodatafono{i}", fpago.codigoDatafon);
        //                        cmd.Parameters.AddWithValue($"@redencion{i}", fpago.redencion);
        //                    }

        //                    cmd.ExecuteNonQuery();
        //                    tx.Commit();
        //                    registrosInsertados += registrosValidosEnLote.Count;
        //                    Console.WriteLine($"✓ Lote insertado: {registrosValidosEnLote.Count} registros");
        //                }
        //                else
        //                {
        //                    tx.Rollback();
        //                    Console.WriteLine("⚠ Lote omitido: no hay registros válidos");
        //                }
        //            }
        //        }
        //    }

        //    cronometer.Stop();
        //    Console.WriteLine($"\n=== RESUMEN ===");
        //    Console.WriteLine($"Registros insertados: {registrosInsertados}");
        //    Console.WriteLine($"Registros omitidos: {registrosOmitidos}");
        //    Console.WriteLine($"Total procesados: {fpago_list.Count}");
        //    Console.WriteLine($"Tiempo: {cronometer.Elapsed.TotalSeconds:F2} segundos");
        //}

        public static async Task insertarFpago(List<t_fpago> fpago_list, int batchSize = 100)
        {
            try
            {
                // Validación inicial
                if (fpago_list == null || !fpago_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {



                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();



                    // se crean la variables de memoria 



                    var cajasMovimientos = new Dictionary<string, int>();
                    var formasPago = new Dictionary<string, int>();

                    //var mercaPesosPorCodigo  = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT numerofactura, id FROM public.""cajasmovimientos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string numeroFactura = reader.GetString(0).Trim();
                            int id = reader.GetInt32(1);

                            if (!cajasMovimientos.ContainsKey(numeroFactura))
                                cajasMovimientos[numeroFactura] = id;
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT ""Codigo"", ""Id"" FROM public.""FormasPago""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigo = reader.GetString(0).Trim();
                            int id = reader.GetInt32(1);

                            if (!formasPago.ContainsKey(codigo))
                                formasPago[codigo] = id;
                        }
                    }
                    while (contador < fpago_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                            INSERT INTO cajasmovimientosformasdepago (
                            cajasmovimientosid, formadepagoid, valor, nombreformapago, codigodatafono,
                            redencion)VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < fpago_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = fpago_list[contador];

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"
                                (
                                    @numero{contador},
                                    @formadepagoId{contador},
                                    @valor{contador}, @nombreFpago{contador}, 
                                    @codigodatafono{contador}, @redencion{contador}

                                )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var mercap = fpago_list[batchStartIndex + i];

                                    //System.Diagnostics.Debugger.Break();
                                    //if (i == 10)
                                    //{
                                    //    System.Diagnostics.Debugger.Break();
                                    //}


                                    // Validación adicional
                                    if (mercap == null) continue;

                                    //cmd.Parameters.AddWithValue(
                                    //    $"@IdTercero{batchStartIndex + i}",
                                    //   (mercap.IdTercero.HasValue && terceros.TryGetValue(mercap.IdTercero.Value, out var idT))
                                    //       ? (object)idT
                                    //       : DBNull.Value
                                    //);


                                    cmd.Parameters.AddWithValue(
                                        $"@numero{batchStartIndex + i}",
                                        (mercap.numero != 0 &&
                                         cajasMovimientos.TryGetValue(mercap.numero.ToString(), out var idCaja))
                                            ? (object)idCaja
                                            : DBNull.Value
                                    );

                                    cmd.Parameters.AddWithValue(
                                        $"@formadepagoId{batchStartIndex + i}",
                                        (!string.IsNullOrWhiteSpace(mercap.formadepagoId) &&
                                         formasPago.TryGetValue(mercap.formadepagoId.Trim(), out var idFP))
                                            ? (object)idFP
                                            : DBNull.Value
                                    );

                                    cmd.Parameters.AddWithValue($"@valor{batchStartIndex + i}", mercap.valor);
                                    cmd.Parameters.AddWithValue($"@nombreFpago{batchStartIndex + i}", mercap.nombreFpago);
                                    cmd.Parameters.AddWithValue($"@codigodatafono{batchStartIndex + i}", mercap.codigoDatafon);
                                    cmd.Parameters.AddWithValue($"@redencion{batchStartIndex + i}", mercap.redencion);
                                    //  cmd.Parameters.AddWithValue(
                                    //    $"@Estado{batchStartIndex + i}",
                                    //    mercap.Estado == 1 ? true : false
                                    //);

                                    //cmd.Parameters.AddWithValue($"@Aceptacion{batchStartIndex + i}", mercap.Aceptacion);


                                }

                                Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                Console.WriteLine($"Lote completado. Registros procesados: {contador}/{fpago_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
            }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarMercapesos(List<t_mercapesos> merca_list, int batchSize = 100)
        {
            try
            {
                // Validación inicial
                if (merca_list == null || !merca_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {



                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();



                    // se crean la variables de memoria 
                    
                    
                    var terceros = new Dictionary<long, int>();
                    
                    //var mercaPesosPorCodigo  = new Dictionary<string, int>();
                    
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Identificacion"", ""Id"" FROM ""Terceros""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            long identificacion = reader.GetInt64(0);
                            int id = reader.GetInt32(1);
                            terceros[identificacion] = id;
                        }
                    }

                    while (contador < merca_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                            INSERT INTO public.""MercaPesos""(
                                ""IdTercero"",
		                        ""Codigo"",
		                        ""Tarjeta"",
		                        ""Acumulado"",
		                        ""Observaciones"",
		                        ""Estado"",
		                        ""Aceptacion"",
		                        ""FechaCreacion"",
		                        ""Firma"")VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < merca_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = merca_list[contador];

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"
                                (
                                    @IdTercero{contador},
		                            @Codigo{contador},
		                            @Tarjeta{contador},
		                            @Acumulado{contador},
		                            @Observaciones{contador},
		                            @Estado{contador},
		                            @Aceptacion{contador},
		                            @FechaCreacion{contador},
		                            @Firma{contador}                                    

                                )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var mercap = merca_list[batchStartIndex + i];


                                    //if (i == 10)
                                    //{
                                    //    System.Diagnostics.Debugger.Break();
                                    //}


                                    // Validación adicional
                                    if (mercap == null) continue;

                                    //cmd.Parameters.AddWithValue(
                                    //    $"@IdTercero{batchStartIndex + i}",
                                    //   (mercap.IdTercero.HasValue && terceros.TryGetValue(mercap.IdTercero.Value, out var idT))
                                    //       ? (object)idT
                                    //       : DBNull.Value
                                    //);


                                    object idTerceroValue = DBNull.Value;
                                    if (mercap.IdTercero.HasValue && terceros != null)
                                    {
                                        if (terceros.TryGetValue(mercap.IdTercero.Value, out var idT))
                                            idTerceroValue = idT;
                                        else
                                            Console.WriteLine($"⚠ No se encontró IdTercero {mercap.IdTercero.Value} en diccionario. Registro {batchStartIndex + i}");
                                    }

                                    cmd.Parameters.AddWithValue($"@IdTercero{batchStartIndex + i}", idTerceroValue ?? DBNull.Value);




                                    cmd.Parameters.AddWithValue($"@Codigo{batchStartIndex + i}", mercap.Codigo);
                                    cmd.Parameters.AddWithValue($"@Tarjeta{batchStartIndex + i}", mercap.Tarjeta);
                                    cmd.Parameters.AddWithValue($"@Acumulado{batchStartIndex + i}", mercap.Acumulado);
                                    cmd.Parameters.AddWithValue($"@Observaciones{batchStartIndex + i}", mercap.Observaciones);
                                    cmd.Parameters.AddWithValue($"@Estado{batchStartIndex + i}", mercap.Estado);
                                    //  cmd.Parameters.AddWithValue(
                                    //    $"@Estado{batchStartIndex + i}",
                                    //    mercap.Estado == 1 ? true : false
                                    //);

                                    //cmd.Parameters.AddWithValue($"@Aceptacion{batchStartIndex + i}", mercap.Aceptacion);

                                    cmd.Parameters.AddWithValue(
                                      $"@Aceptacion{batchStartIndex + i}",
                                      mercap.Aceptacion == "1" ? true : false
                                  );

                                    cmd.Parameters.AddWithValue($"@FechaCreacion{batchStartIndex + i}", mercap.FechaCreacion);
                                    cmd.Parameters.AddWithValue($"@Firma{batchStartIndex + i}", (object)DBNull.Value); 

                                }

                                Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                Console.WriteLine($"Lote completado. Registros procesados: {contador}/{merca_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
            }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarMovite(List<t_mov_inventarios> movite_list,List<t_cuen> cuent_list,List<t_cabdoc> cabdocList ,int batchSize = 1000)
        {
            try
            {
                // Validación inicial
                if (movite_list == null || !movite_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {



                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();



                    // se crean la variables de memoria 
                    var sucursales  = new Dictionary<string, (int Id, string Nombre)>();
                    var terceros    = new Dictionary<long, int>();
                    var documentos  = new Dictionary<string, int>();
                    var dictionary  = new Dictionary<int, string>();
                    var usuarios = new Dictionary<string, string>();


                    //var mercaPesosPorCodigo  = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT codigo, id, nombre FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);
                            string nombre = reader.GetString(2);
                            sucursales[codigo] = (id, nombre);
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT ""Identificacion"", ""Id"" FROM ""Terceros""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            long identificacion = reader.GetInt64(0);
                            int id = reader.GetInt32(1);
                            terceros[identificacion] = id;
                        }
                    }


                    // Documentos
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM ""Documentos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            documentos[codigo] = id;
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Descripcion"" FROM public.""PucMaestro""", conn))
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            string descripcion = reader.GetString(1);

                            dictionary[id] = descripcion;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""UserName"" FROM ""AspNetUsers""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string id = reader.GetString(0);       // Id en AspNetUsers casi siempre es string
                            string username = reader.GetString(1); // UserName
                            usuarios[username] = id;
                        }
                    }



                    while (contador < movite_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                            INSERT INTO public.""MovimientosInventarios""(
                                ""IdSucursal"", 
                                ""IdDocumento"", 
                                ""Fecha"", 
                                ""Periodo"", 
                                ""Consecutivo"", 
                                ""IdTercero"", 
                                ""IdCxPagar"", 
                                ""Zona"", 
                                ""Numero"",
                                ""IdUsuario"", 
                                ""IdEstado""
                                )VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < movite_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = movite_list[contador];
                                    string usuario = "";

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                   

                                    var values = $@"
                                (
                                    
                                    @IdSucursal{contador}, 
                                    @IdDocumento{contador},
                                    @Fecha{contador}, 
                                    @Periodo{contador},
                                    @Consecutivo{contador}, 
                                    @IdTercero{contador}, 
                                    @IdCxPagar{contador}, 
                                    @Zona{contador}, 
                                    @Numero{contador},
                                    @IdUsuario{contador}, 
                                    @IdEstado{contador}


                                )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }


                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                bool avisoMostrado = false;


                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = movite_list[batchStartIndex + i];
                                    string usuario = "";
                                    string cuen_final = ""; 
                                    //if (i == 10)
                                    //{
                                    //    System.Diagnostics.Debugger.Break();
                                    //}

                                    var usu = cabdocList.FirstOrDefault(c => c.docum == movi.IdDocumento && c.numero == movi.Numero && c.consecut == movi.Periodo);

                                    if (usu != null)
                                    {
                                        usuario = usu.usuario;
                                    }

                                    var cuen = cuent_list.FirstOrDefault(c => c.cod_ret == movi.IdCxPagar);

                                    if (cuen != null)
                                    {
                                        cuen_final = cuen.cuenta;
                                    }

                                    // Validación adicional
                                    if (movi == null) continue;


                                    object idTerceroValue = DBNull.Value;


                                    if (movi.IdTercero.HasValue && terceros != null)
                                    {
                                        if (terceros.TryGetValue(movi.IdTercero.Value, out var idT))
                                            idTerceroValue = idT;
                                        else if (!avisoMostrado)  // <-- solo la primera vez
                                        {
                                            Console.WriteLine($"⚠ No se encontró IdTercero {movi.IdTercero.Value} en diccionario.");
                                            avisoMostrado = true;
                                        }
                                    }



                                    //if (movi.IdTercero.HasValue && terceros != null)
                                    //{
                                    //    if (terceros.TryGetValue(movi.IdTercero.Value, out var idT))
                                    //        idTerceroValue = idT;
                                    //    else
                                    //        Console.WriteLine($"⚠ No se encontró IdTercero {movi.IdTercero.Value} en diccionario. Registro {batchStartIndex + i}");
                                    //}

                                    cmd.Parameters.AddWithValue($"@IdTercero{batchStartIndex + i}", idTerceroValue ?? DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@IdSucursal{batchStartIndex + i}",
                                    sucursales.ContainsKey(movi.IdSucursal) ? sucursales[movi.IdSucursal].Id : (object)DBNull.Value);


                                    cmd.Parameters.AddWithValue($"@IdDocumento{batchStartIndex + i}", documentos.TryGetValue(movi.IdDocumento, out var doc) ? doc : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@fecha{batchStartIndex + i}", movi.Fecha);
                                    cmd.Parameters.AddWithValue($"@Periodo{batchStartIndex + i}", movi.Periodo);

                                    cmd.Parameters.AddWithValue($"@IdCxPagar{batchStartIndex + i}", documentos.TryGetValue(movi.IdCxPagar, out var cxp) ? cxp : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@Consecutivo{batchStartIndex + i}", i); // hacerlo contador
                                    cmd.Parameters.AddWithValue($"@Zona{batchStartIndex + i}", movi.Zona); // hacerlo contador
                                    cmd.Parameters.AddWithValue($"@Numero{batchStartIndex + i}", movi.Numero);



                                    cmd.Parameters.AddWithValue($"@IdUsuario{batchStartIndex + i}",
                                    usuarios.TryGetValue(usuario, out var userId) ? userId : (object)DBNull.Value);


                                    cmd.Parameters.AddWithValue($"@IdUsuario{batchStartIndex + i}", usuario);
                                    cmd.Parameters.AddWithValue($"@IdEstado{batchStartIndex + i}", 7); 


                                }


                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}..."); 

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();


                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{movite_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
            }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarDetalleMovite(List<t_det_movInventario> movite_list, int batchSize = 1000)
        {
            try
            {
                if (movite_list == null || !movite_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Diccionarios en memoria
                    var productos = new Dictionary<string, int>();
                    var bodegas = new Dictionary<string, int>();
                    var movimientos = new Dictionary<(string Documento, string Numero), int>();

                    // Productos
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Codigo"", ""Id"" FROM ""Productos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);
                            productos[codigo] = id;
                        }
                    }

                    // Bodegas
                    using (var cmd = new NpgsqlCommand(@"SELECT ""codigo"", ""id"" FROM ""bodegas""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);
                            bodegas[codigo] = id;
                        }
                    }

                    // Movimientos (IdDocumento + Numero)
                    using (var cmd = new NpgsqlCommand(@"
                SELECT m.""Id"", d.""Codigo"", m.""Numero""
                FROM ""MovimientosInventarios"" m
                INNER JOIN ""Documentos"" d ON m.""IdDocumento"" = d.""Id""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string docCodigo = reader.GetString(1);
                            string numero = reader.GetString(2);
                            movimientos[(docCodigo, numero)] = id;
                        }
                    }

                    // Procesar en lotes
                    while (contador < movite_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""DetalleMovimientosInventarios""(
                                    ""IdMovimiento"", 
                                    ""IdProducto"", 
                                    ""IdBodega"", 
                                    ""Cantidad"", 
                                    ""Recibida"", 
                                    ""CostoUnitarioBruto"", 
                                    ""CostoUnitarioNeto"", 
                                    ""CostoTotal"", 
                                    ""PorcentajeIVA"", 
                                    ""ValorIVA"", 
                                    ""ImpuestoConsumo"", 
                                    ""FechaCreacion""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < movite_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = movite_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @IdMovimiento{contador},
                                        @IdProducto{contador},
                                        @IdBodega{contador},
                                        @Cantidad{contador},
                                        @Recibida{contador},
                                        @CostoUnitarioBruto{contador},
                                        @CostoUnitarioNeto{contador},
                                        @CostoTotal{contador},
                                        @PorcentajeIVA{contador},
                                        @ValorIVA{contador},
                                        @ImpuestoConsumo{contador},
                                        @FechaCreacion{contador}
                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = movite_list[batchStartIndex + i];
                                    if (movi == null) continue;

                                    // IdMovimiento
                                    cmd.Parameters.AddWithValue($"@IdMovimiento{batchStartIndex + i}",
                                        movimientos.ContainsKey((movi.documento, Convert.ToString(movi.numero))) ? movimientos[(movi.documento, Convert.ToString(movi.numero))] : (object)DBNull.Value);

                                    // IdProducto
                                    cmd.Parameters.AddWithValue($"@IdProducto{batchStartIndex + i}",
                                        productos.ContainsKey(movi.IdProducto) ? productos[movi.IdProducto] : (object)DBNull.Value);

                                    // IdBodega
                                    cmd.Parameters.AddWithValue($"@IdBodega{batchStartIndex + i}",
                                        bodegas.ContainsKey(movi.IdBodega) ? bodegas[movi.IdBodega] : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@Cantidad{batchStartIndex + i}", movi.Cantidad);
                                    cmd.Parameters.AddWithValue($"@Recibida{batchStartIndex + i}", movi.Recibida);
                                    cmd.Parameters.AddWithValue(
                                     $"@CostoUnitarioBruto{batchStartIndex + i}",
                                     movi.Cantidad == 0 ? movi.CostoUnitarioBruto : movi.CostoUnitarioBruto / movi.Cantidad
                                 );
                                    cmd.Parameters.AddWithValue($"@CostoUnitarioNeto{batchStartIndex + i}", movi.CostoUnitarioNeto);
                                    cmd.Parameters.AddWithValue($"@CostoTotal{batchStartIndex + i}", movi.CostoTotal);
                                    cmd.Parameters.AddWithValue($"@PorcentajeIVA{batchStartIndex + i}", movi.PorcentajeIVA);
                                    cmd.Parameters.AddWithValue($"@ValorIVA{batchStartIndex + i}", movi.ValorIVA);
                                    cmd.Parameters.AddWithValue($"@ImpuestoConsumo{batchStartIndex + i}", movi.ImpuestoConsumo);
                                    cmd.Parameters.AddWithValue($"@FechaCreacion{batchStartIndex + i}", movi.FechaCreacion);
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{movite_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarDetalleMovdoc(List<t_det_movContable> movdoc_list, int batchSize = 1000)
        {
            try
            {
                if (movdoc_list == null || !movdoc_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Diccionarios en memoria
                    
                    
                    var zonas = new Dictionary<int, string>();

                    var movimientos = new Dictionary<(string Sucursal, string Documento, DateTime fecha, string anexo, string periodo), int>();
                    var cuenta = new Dictionary<string, string>();
                    var actividadesDict = new Dictionary<string ,int>();
                    var vendedores = new Dictionary<string, int>();     // codigo → Id


                    // Movimientos (IdDocumento + Numero)

                    using (var cmd = new NpgsqlCommand(@"
                    SELECT ""Id"", ""Sucursal"", ""Documento"", ""Fecha"",""Tercero"",""Periodo""
                    FROM ""MovimientosContables""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id             = reader.GetInt32(0);
                            string sucursal    = reader.GetString(1);
                            string idDocumento = reader.GetString(2);
                            DateTime fecha     = reader.GetDateTime(3);
                            string  tercero    = reader.GetString(4);
                            string periodo     = reader.GetString(5);

                            movimientos[(sucursal, idDocumento, fecha, tercero,periodo)] = id;
                        }
                    }

                    // Cargar diccionario de Vendedores
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM public.vendedores", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);          // id
                            string codigo = reader.GetString(1);  // codigo (string en la tabla)

                            vendedores[codigo] = id;              // asignación correcta
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Codigo"" ,""Id"" FROM public.""ActividadesDIAN""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);   

                            actividadesDict[codigo] = id;
                        }
                    }


                    

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Descripcion"", ""CodCue"" FROM public.""PucMaestro""", conn))
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string Descripcion = reader.GetString(0);
                            string CodCue      = reader.GetString(1);

                            cuenta[CodCue] = Descripcion;
                        }
                    }





                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Descripcion"" FROM ""Zonas""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string descripcion = reader.GetString(1);
                            zonas[id] = descripcion;
                        }
                    }
                    // Procesar en lotes
                    while (contador < movdoc_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""DetalleMovimientosContables""(
                                ""IdMovimiento"", 
                                ""Cuenta"", 
                                ""NombreCuenta"",
                                ""Base"",
                                ""Debe"", 
                                ""Haber"",
                                ""CodigoPPYE"", 
                                ""Anexo"", 
                                ""DocReferencia"", 
                                ""FechaVencimiento"", 
                                ""IdCodigoICA"", 
                                ""IdVendedor"", 
                                ""IdZona"",
                                ""Documento"", 
                                ""Sucursal""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < movdoc_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = movdoc_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        @IdMovimiento{contador},
                                        @Cuenta{contador},
                                        @NombreCuenta{contador},
                                        @Base{contador},
                                        @Debe{contador},
                                        @Haber{contador},
                                        @CodigoPPYE{contador},
                                        @Anexo{contador},
                                        @DocReferencia{contador},
                                        @FechaVencimiento{contador},
                                        @IdCodigoICA{contador},
                                        @IdVendedor{contador},
                                        @IdZona{contador},                                        
                                        @Documento{contador},
                                        @Sucursal{contador}

                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = movdoc_list[batchStartIndex + i];
                                    string descripcionCuenta;
                                    int idcodigoIca = 0;
                                    
                                    if (movi == null) continue;


                                    if (!string.IsNullOrEmpty(movi.Cuenta) && cuenta.ContainsKey(movi.Cuenta))
                                    {
                                        descripcionCuenta = cuenta[movi.Cuenta];
                                    }
                                    else
                                    {
                                        descripcionCuenta = null;
                                    }



                                    if (actividadesDict != null)
                                    {
                                        if (actividadesDict.TryGetValue(movi.IdCodigoICA.ToString(), out var idT))
                                        {
                                            idcodigoIca = idT;
                                        }
                                    }

                                    // IdMovimiento
                                    //cmd.Parameters.AddWithValue($"@IdMovimiento{batchStartIndex + i}",
                                    //movimientos.ContainsKey((
                                    //    movi.Sucursal, // Sucursal (int)
                                    //    movi.Documento,                 // Documento (string)
                                    //    i                               // Consecutivo (int, viene del for)
                                    //))
                                    //    ? movimientos[(
                                    //        movi.Sucursal,
                                    //        movi.Documento,
                                    //        i
                                    //      )]
                                    //    : (object)DBNull.Value);

                                    cmd.Parameters.Add($"@IdMovimiento{batchStartIndex + i}", NpgsqlTypes.NpgsqlDbType.Integer).Value =
                                    movimientos.TryGetValue((movi.Sucursal, movi.Documento, movi.Fecha,movi.Anexo, movi.periodo), out int idMovimiento)
                                        ? idMovimiento
                                        : (object)DBNull.Value;

                                    cmd.Parameters.AddWithValue($"@Cuenta{batchStartIndex + i}", movi.Cuenta);
                                    // Nombrecuenta
                                    cmd.Parameters.AddWithValue($"@NombreCuenta{batchStartIndex + i}", descripcionCuenta);
                                    cmd.Parameters.AddWithValue($"@Base{batchStartIndex + i}", movi.Base);
                                    cmd.Parameters.AddWithValue($"@Debe{batchStartIndex + i}", movi.Debe);
                                    cmd.Parameters.AddWithValue($"@Haber{batchStartIndex + i}", movi.Haber);
                                    cmd.Parameters.AddWithValue($"@CodigoPPYE{batchStartIndex + i}", movi.CodigoPPYE);
                                    cmd.Parameters.AddWithValue($"@Anexo{batchStartIndex + i}", movi.Anexo);
                                    cmd.Parameters.AddWithValue($"@DocReferencia{batchStartIndex + i}", movi.@DocReferencia);
                                    cmd.Parameters.AddWithValue($"@FechaVencimiento{batchStartIndex + i}", movi.FechaVencimiento);
                                    cmd.Parameters.AddWithValue($"@IdCodigoICA{batchStartIndex + i}", idcodigoIca);
                                    cmd.Parameters.AddWithValue(
                                        $"@IdVendedor{batchStartIndex + i}",
                                        vendedores.ContainsKey(movi.IdVendedor)
                                            ? vendedores[movi.IdVendedor]
                                            : (object)DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue(
                                         $"@IdZona{batchStartIndex + i}",
                                         int.TryParse(movi.IdZona, out int valorZona) && valorZona != 0
                                             ? valorZona
                                             : (object)DBNull.Value
                                     );


                                    // cmd.Parameters.AddWithValue($"@ClasePPYE{batchStartIndex + i}", movi.ClasePPYE);
                                    cmd.Parameters.AddWithValue($"@Documento{batchStartIndex + i}", movi.Documento);
                                    cmd.Parameters.AddWithValue($"@Sucursal{batchStartIndex + i}", movi.Sucursal);

                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{movdoc_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarInventario(List<t_inventario> inventario_list, int batchSize = 1000)
        {
            try
            {
                if (inventario_list == null || !inventario_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Diccionarios en memoria
                    var sucursales = new Dictionary<string, (int Id, string Nombre)>();
                    var productos = new Dictionary<string, int>();     // codigo → Id
                    var bodegas = new Dictionary<string, int>();
                    // Movimientos (IdDocumento + Numero)

               

                    using (var cmd = new NpgsqlCommand(@"SELECT codigo, id, nombre FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);
                            string nombre = reader.GetString(2);
                            sucursales[codigo] = (id, nombre);
                        }
                    }
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM public.""Productos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);

                            productos[codigo] = id;
                        }
                    }
                    using (var cmd = new NpgsqlCommand(@"SELECT ""codigo"", ""id"" FROM ""bodegas""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);
                            bodegas[codigo] = id;
                        }
                    }

                    // Procesar en lotes
                    while (contador < inventario_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""Inventarios""(
                                 ""IdSucursal"", 
                                 ""IdProducto"", 
                                 ""Cantidad"", 
                                 ""IdBodega"", 
                                 ""FechaCorte""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < inventario_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = inventario_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        
                                        @IdSucursal{contador}, 
                                        @IdProducto{contador},
                                        @Cantidad{contador}, 
                                        @IdBodega{contador}, 
                                        @FechaCorte{contador}

                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = inventario_list[batchStartIndex + i];


                                    if (movi == null) continue;


                
                                     cmd.Parameters.AddWithValue($"@IdSucursal{batchStartIndex + i}",
                                     sucursales.ContainsKey(movi.IdSucursal) ? sucursales[movi.IdSucursal].Id : (object)DBNull.Value);

                                    if (!productos.ContainsKey(movi.IdProducto))
                                    {
                                        Console.ForegroundColor = ConsoleColor.Yellow;
                                        Console.WriteLine($"[NO ENCONTRADO] Producto '{movi.IdProducto}' no existe en la tabla Productos (índice {batchStartIndex + i})");
                                        Console.ResetColor();

                                        // Evitar insertar nulo en base de datos
                                        continue;
                                    }


                                    cmd.Parameters.AddWithValue(
                                       $"@IdProducto{batchStartIndex + i}",
                                       productos.ContainsKey(movi.IdProducto)
                                           ? productos[movi.IdProducto]
                                           : (object)DBNull.Value
                                    );


                                    cmd.Parameters.AddWithValue($"@IdBodega{batchStartIndex + i}",
                                        bodegas.ContainsKey(movi.IdBodega) ? bodegas[movi.IdBodega] : (object)DBNull.Value);

                                  
                                    cmd.Parameters.AddWithValue($"@Cantidad{batchStartIndex + i}", movi.Cantidad);
                                
                                    cmd.Parameters.AddWithValue($"@FechaCorte{batchStartIndex + i}", movi.FechaCorte);
                                    

                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{inventario_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarElementosPPYE(List<t_Ele_PPYE> ppe_list, int batchSize = 2000)
        {
            try
            {
                if (ppe_list == null || !ppe_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Diccionarios en memoria
                    
                    var GruposContables = new Dictionary<string, int>();
                    var dictionary = new Dictionary<string, int>();
                    var usuarios = new Dictionary<string, string>();
                    var categoria = new Dictionary<(string Categoria,string SubCategoria), int>();
                    


                    // Movimientos (IdDocumento + Numero)


                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM public.""GruposContables""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);

                            GruposContables[codigo] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""CodCue"",""Id""  FROM public.""PucMaestro""", conn))
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string codcue = reader.GetString(0);
                            int id = reader.GetInt32(1);

                            dictionary[codcue] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Categoria"",""SubCategoria"",""Id"" FROM ""CategoriasContabilidad""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string Categoria = reader.IsDBNull(0) ? string.Empty : reader.GetString(0).Trim().ToUpperInvariant();
                            string SubCategoria = reader.IsDBNull(1) ? string.Empty : reader.GetString(1).Trim().ToUpperInvariant();
                            int id = reader.GetInt32(2);
                            categoria[(Categoria, SubCategoria)] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""UserName"" FROM ""AspNetUsers""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string id = reader.GetString(0);       // Id en AspNetUsers casi siempre es string
                            string username = reader.GetString(1); // UserName
                            usuarios[username] = id;
                        }
                    }


                    //SELECT "Id", "Categoria"

                    // Procesar en lotes
                    while (contador < ppe_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""ElementoPPYE""(
                                 ""IdCategoria"", 
                                 ""CodCategoria"", 
                                 ""Elemento"", 
                                 ""Ref1"", 
                                 ""Ref2"", 
                                 ""IdGrupoContable"", 
                                 ""IdCuenta"", 
                                 ""CodAgrupacion"", 
                                 ""FechaCreacion"", 
                                 ""IdUsuario"", 
                                 ""ConsCodigo""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < ppe_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = ppe_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        
                                       @IdCategoria{contador},
                                       @CodCategoria{contador},                                        
                                       @Elemento{contador}, 
                                       @Ref1{contador}, 
                                       @Ref2{contador}, 
                                       @IdGrupoContable{contador}, 
                                       @IdCuenta{contador}, 
                                       @CodAgrupacion{contador}, 
                                       @FechaCreacion{contador}, 
                                       @IdUsuario{contador}, 
                                       @ConsCodigo{contador}
   

                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = ppe_list[batchStartIndex + i];

                                    string user = "CARLOSM";
                                    if (movi == null) continue;


                                    if (categoria.TryGetValue((movi.IdCategoria, movi.scat_ppe), out int idCategoria))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdCategoria{batchStartIndex + i}", idCategoria);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró categoría para ({movi.IdCategoria}, {movi.scat_ppe}, {movi.sbcat_ppe})");
                                        cmd.Parameters.AddWithValue($"@IdCategoria{batchStartIndex + i}", DBNull.Value);
                                    }

                                    //cmd.Parameters.AddWithValue($"@IdCategoria{batchStartIndex + i}",
                                    // categoria.ContainsKey((movi.IdCategoria,movi.scat_ppe,movi.sbcat_ppe))
                                    //     ? categoria[(movi.IdCategoria, movi.scat_ppe, movi.sbcat_ppe)]
                                    //     : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@CodCategoria{batchStartIndex + i}", movi.CodCategoria);
                                    cmd.Parameters.AddWithValue($"@Elemento{batchStartIndex + i}", "0");
                                    cmd.Parameters.AddWithValue($"@Ref1{batchStartIndex + i}", movi.Ref1);
                                    cmd.Parameters.AddWithValue($"@Ref2{batchStartIndex + i}", movi.Ref2);

                                    cmd.Parameters.AddWithValue($"@IdGrupoContable{batchStartIndex + i}",
                                    GruposContables.ContainsKey(movi.IdGrupoContable)
                                      ? GruposContables[movi.IdGrupoContable]
                                      : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@IdCuenta{batchStartIndex + i}",
                                    dictionary.ContainsKey(movi.IdCuenta)
                                      ? dictionary[movi.IdCuenta]
                                      : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@CodAgrupacion{batchStartIndex + i}", movi.CodAgrupacion);
                                    cmd.Parameters.AddWithValue($"@FechaCreacion{batchStartIndex + i}", DateTime.Now);
                                    cmd.Parameters.AddWithValue($"@IdUsuario{batchStartIndex + i}",
                                    usuarios.TryGetValue(user, out var userId) ? userId : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@ConsCodigo{batchStartIndex + i}", i);

                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{ppe_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarTelibro(List<t_telibro> telibro_lst, int batchSize = 2000)
        {
            try
            {
                if (telibro_lst == null || !telibro_lst.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Diccionarios en memoria
                    var LibroTesoreriaDetalles = new Dictionary<DateOnly, int>();
                    var formaspago = new Dictionary<string, int>();
                    var sucursales = new Dictionary<string, (int Id, string Nombre)>();
                    var documentos = new Dictionary<string, int>();
                    var terceros = new Dictionary<long, int>();


                    using (var cmd = new NpgsqlCommand(@"
                        SELECT d.""Id"", l.""FechaCaptura""
                        FROM ""LibroDiaTesoreriaDetalles"" d
                        INNER JOIN ""LibroDiaTesoreria"" l ON d.""IdLibroTesoreria"" = l.""Id""
                    ", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int idDetalle = reader.GetInt32(0);
                            DateTime fecha = reader.GetDateTime(1);

                            // Convertir a DateOnly si tu modelo usa DateOnly
                            DateOnly fechaCaptura = DateOnly.FromDateTime(fecha);

                            LibroTesoreriaDetalles[fechaCaptura] = idDetalle;
                        }
                    }


                    // Movimientos (IdDocumento + Numero)

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM ""FormasPago""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            formaspago[codigo] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT codigo, id, nombre FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);
                            string nombre = reader.GetString(2);
                            sucursales[codigo] = (id, nombre);
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM public.""Documentos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);

                            documentos[codigo] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Identificacion"", ""Id"" FROM ""Terceros""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            long identificacion = reader.GetInt64(0);
                            int id = reader.GetInt32(1);
                            terceros[identificacion] = id;
                        }
                    }

                    //SELECT "Id", "Categoria"

                    // Procesar en lotes
                    while (contador < telibro_lst.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""LibroDiaTesoreriaConceptos""(
                                ""IdLibroDetalle"", ""FormaPago"", ""Tipo"", ""Concepto"", ""Cantidad"", ""Valor"", ""ValorDefault"", ""IdFPago"",
                                ""Naturaleza"", ""IdSucursal"", ""IdDocumento"", ""Periodo"", ""Consecutivo"", ""IdTercero""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < telibro_lst.Count && currentBatchSize < batchSize)
                                {
                                    var mov = telibro_lst[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        
                                     @IdLibroDetalle{contador},
                                     @formapago{contador}, @Tipo{contador}, @concepto{contador}, @Cantidad{contador}, @valor{contador}, @ValorDefault{contador},
                                     @IdFPago{contador}, @naturaleza{contador},
                                     @IdSucursal{contador},
                                     @IdDocumento{contador}, @Periodo{contador}, @Consecutivo{contador},
                                     @IdTercero{contador}
  
                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var tlibro = telibro_lst[batchStartIndex + i];

                                    
                                    if (tlibro == null) continue;



                                    object valorDetalle = DBNull.Value;

                                    // si tlibro.Fecha es DateTime:
                                    if (tlibro.fecha is DateTime fechaDT)
                                    {
                                        var key = DateOnly.FromDateTime(fechaDT);
                                        if (LibroTesoreriaDetalles.TryGetValue(key, out var idDetalle))
                                            valorDetalle = idDetalle;
                                    }

                                    cmd.Parameters.AddWithValue($"@IdLibroDetalle{batchStartIndex + i}", valorDetalle);

                                    cmd.Parameters.AddWithValue($"@formapago{batchStartIndex + i}", string.IsNullOrWhiteSpace(tlibro.fpago) ? DBNull.Value : tlibro.fpago.Trim());


                                    cmd.Parameters.AddWithValue($"@Tipo{batchStartIndex + i}", tlibro.tipofp);

                                    cmd.Parameters.AddWithValue($"@concepto{batchStartIndex + i}", tlibro.concepto.Trim());
                                    cmd.Parameters.AddWithValue($"@Cantidad{batchStartIndex + i}", tlibro.cant == 0 ? DBNull.Value : tlibro.cant);
                                    cmd.Parameters.AddWithValue($"@valor{batchStartIndex + i}", tlibro.valor == 0 ? DBNull.Value : tlibro.valor);
                                    cmd.Parameters.AddWithValue($"@ValorDefault{batchStartIndex + i}", tlibro.valordef == 0 ? 0 : tlibro.valordef);
                                    cmd.Parameters.AddWithValue(
                                        $"@IdFPago{batchStartIndex + i}",
                                        (!string.IsNullOrWhiteSpace(tlibro.formapago) &&
                                         formaspago.TryGetValue(tlibro.formapago.Trim(), out var idFP))
                                            ? (object)idFP
                                            : DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue($"@naturaleza{batchStartIndex + i}", tlibro.naturaleza.Trim());

                                    cmd.Parameters.AddWithValue($"@IdSucursal{batchStartIndex + i}",
                                    sucursales.ContainsKey(tlibro.sucursal) ? sucursales[tlibro.sucursal].Id : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@IdDocumento{batchStartIndex + i}",
                                      documentos.ContainsKey(tlibro.docum)
                                     ? documentos[tlibro.docum]
                                     : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@Periodo{batchStartIndex + i}", string.IsNullOrWhiteSpace(tlibro.consecut) ? DBNull.Value : tlibro.consecut.Trim());
                                    cmd.Parameters.AddWithValue($"@Consecutivo{batchStartIndex + i}", i);
                                    cmd.Parameters.AddWithValue(
                                    $"@IdTercero{batchStartIndex + i}",
                                    (tlibro.nit.HasValue && terceros.TryGetValue(tlibro.nit.Value, out var ids))
                                    ? (object)ids
                                    : DBNull.Value
      
                                   );
                                    //cmd.Parameters.AddWithValue("nit", tlibro.nit == 0 ? DBNull.Value : tlibro.nit);
                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{telibro_lst.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarFacturaPro(List<t_facturasPro> fact_list, List<t_cuen> cuent_list, int batchSize = 2000)
        {
            try
            {
                if (fact_list == null || !fact_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Diccionarios en memoria
                    var sucursales = new Dictionary<string, (int Id, string Nombre)>();
                    var documentos = new Dictionary<string, int>();
                    var proveedor  = new Dictionary<long, int>();
                    var usuarios = new Dictionary<string, string>();
                    var proveedores = new Dictionary<long, int>();
                    var dictionary = new Dictionary<string, int>();
                    var ordenes = new Dictionary<string, int>();
                    var bodegas = new Dictionary<string, int>();


                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM bodegas", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            bodegas[codigo] = id;
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT codigo, id, nombre FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);
                            string nombre = reader.GetString(2);
                            sucursales[codigo] = (id, nombre);
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM public.""Documentos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);

                            documentos[codigo] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Identificacion"",""Id""  FROM public.""Terceros""", conn))
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            long Identificacion = reader.GetInt64(0);
                            int id = reader.GetInt32(1);

                            proveedor[Identificacion] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM public.""Documentos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);

                            documentos[codigo] = id;
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""CodCue"" FROM public.""PucMaestro""", conn))
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            string CodCue = reader.GetString(1);

                            dictionary[CodCue] = id;
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT id, numeroordencompra FROM public.""ordenesdecompra""", conn))
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            string numeroordencompra = reader.GetString(1).Trim();

                            ordenes[numeroordencompra] =id ;
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""UserName"" FROM ""AspNetUsers""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string id = reader.GetString(0);       // Id en AspNetUsers casi siempre es string
                            string username = reader.GetString(1); // UserName
                            usuarios[username] = id;
                        }
                    }


                    //SELECT "Id", "Categoria"

                    // Procesar en lotes
                    while (contador < fact_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""FacturasProductos""(
                                ""Numero"", 
                                ""Fecha"", 
                                ""IdProveedor"", 
                                ""IdDocumento"", 
                                ""IdEstado"", 
                                ""IdSucursal"", 
                                ""IdOrdenCompra"", 
                                ""Subtotal"", 
                                ""Total"", 
                                ""CxPagar"",
                                ""DiasCredito"",
                                ""FechaRecibido"", 
                                ""Observaciones"", 
                                ""IVAFletes"", 
                                ""IdBodega"", 
                                ""IdCentroCosto"",
                                ""Contado"",
                                ""Receptor"", 
                                ""Entradas"", 
                                ""IConsumo"", 
                                ""Flete"", 
                                ""ValorIVA"", 
                                ""ValorRetencion"", 
                                ""Fenaice"", 
                                ""ReteICA"", 
                                ""IdRFte"", 
                                ""RFte"", 
                                ""Asohofrucol"", 
                                ""FNFP"", 
                                ""Periodo"", 
                                ""Consecutivo"", 
                                ""IdUsuarioElaboro""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < fact_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = fact_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        
                                     @Numero{contador},
                                     @Fecha{contador},
                                     @IdProveedor{contador},
                                     @IdDocumento{contador},
                                     @IdEstado{contador},
                                     @IdSucursal{contador},
                                     @IdOrdenCompra{contador},
                                     @Subtotal{contador},
                                     @Total{contador},
                                     @CxPagar{contador},
                                     @DiasCredito{contador},
                                     @FechaRecibido{contador},
                                     @Observaciones{contador},
                                     @IVAFletes{contador},
                                     @IdBodega{contador},
                                     @IdCentroCosto{contador},
                                     @Contado{contador},
                                     @Receptor{contador},
                                     @Entradas{contador},
                                     @IConsumo{contador},
                                     @Flete{contador},
                                     @ValorIVA{contador},
                                     @ValorRetencion{contador},
                                     @Fenaice{contador},
                                     @ReteICA{contador},
                                     @IdRFte{contador},
                                     @RFte{contador},
                                     @Asohofrucol{contador},
                                     @FNFP{contador},
                                     @Periodo{contador},
                                     @Consecutivo{contador},
                                     @IdUsuarioElaboro{contador}
                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = fact_list[batchStartIndex + i];

                                    var cuen = cuent_list.FirstOrDefault(c => c.cod_ret == movi.CxP);

                                    var cuen_final = "";

                                    if (cuen != null)
                                    {
                                        cuen_final = cuen.cuenta;
                                    }



                                    if (!ordenes.TryGetValue(movi.IdOrdenCompra.Trim(), out int idOrden))
                                    {

                                        //Console.WriteLine($"⚠️ No se encontró IdOrdenCompra para el valor ({movi.IdOrdenCompra})");
                                        Console.WriteLine($"{movi.IdOrdenCompra}");
                                        // Si necesitas enviar NULL en caso de no existir:
                                        // cmd.Parameters.AddWithValue($"@IdOrdenCompra{batchStartIndex + i}", DBNull.Value);
                                        continue;
                                    }



                                    string user = "CARLOSM";
                                    if (movi == null) continue;

                                    cmd.Parameters.AddWithValue($"@Numero{batchStartIndex + i}", movi.Numero);
                                    cmd.Parameters.AddWithValue($"@Fecha{batchStartIndex + i}", movi.Fecha);


                                    cmd.Parameters.AddWithValue(
                                        $"@IdProveedor{batchStartIndex + i}",
                                       (movi.IdProveedor.HasValue && proveedor.TryGetValue(movi.IdProveedor.Value, out var idT))
                                           ? (object)idT
                                           : DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue($"@IdDocumento{batchStartIndex + i}",
                                     documentos.ContainsKey(movi.IdDocumento)
                                   ? documentos[movi.IdDocumento]
                                   : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@IdEstado{batchStartIndex + i}", 7);

                                    cmd.Parameters.AddWithValue($"@IdSucursal{batchStartIndex + i}",
                                    sucursales.ContainsKey(movi.IdSucursal) ? sucursales[movi.IdSucursal].Id : (object)DBNull.Value);

                                   // Console.WriteLine("Orden #= " + movi.IdOrdenCompra);


                                    cmd.Parameters.AddWithValue($"@Subtotal{batchStartIndex + i}", movi.Subtotal);
                                    cmd.Parameters.AddWithValue($"@Total{batchStartIndex + i}", movi.Total);

                                    cmd.Parameters.AddWithValue($"@CxPagar{batchStartIndex + i}", dictionary.TryGetValue(cuen_final, out var cxp) ? cxp : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@IdRFte{batchStartIndex + i}", dictionary.TryGetValue(movi.CxPagar, out var cxps) ? cxps : (object)DBNull.Value);


                                    cmd.Parameters.AddWithValue($"@DiasCredito{batchStartIndex + i}", movi.DiasCredito);

                                    if (ordenes.TryGetValue(movi.IdOrdenCompra.Trim(), out int idOrden2))
                                    {

                                        cmd.Parameters.AddWithValue($"@IdOrdenCompra{batchStartIndex + i}", idOrden2);
                                    }


                                    cmd.Parameters.AddWithValue($"@FechaRecibido{batchStartIndex + i}", movi.Fecha);

                                    cmd.Parameters.AddWithValue($"@Observaciones{batchStartIndex + i}", movi.Observaciones);

                                    cmd.Parameters.AddWithValue($"@IVAFletes{batchStartIndex + i}", movi.IVAFletes);

                                    cmd.Parameters.AddWithValue($"@IdCentroCosto{batchStartIndex + i}", bodegas.TryGetValue(movi.IdCentroCosto, out var cc) ? cc : (object)DBNull.Value);

                                    
                                    cmd.Parameters.AddWithValue($"@Contado{batchStartIndex + i}", movi.Contado);
                                    
                                    cmd.Parameters.AddWithValue($"@IdBodega{batchStartIndex + i}", bodegas.TryGetValue(movi.IdBodega, out var bod) ? bod : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@Receptor{batchStartIndex + i}", movi.Receptor);
                                    cmd.Parameters.AddWithValue($"@Entradas{batchStartIndex + i}", movi.Entradas);
                                    cmd.Parameters.AddWithValue($"@IConsumo{batchStartIndex + i}", movi.@IConsumo);
                                    cmd.Parameters.AddWithValue($"@Flete{batchStartIndex + i}", movi.Flete);
                                    
                                    cmd.Parameters.AddWithValue($"@ValorIVA{batchStartIndex + i}", movi.ValorIVA);
                                    cmd.Parameters.AddWithValue($"@ValorRetencion{batchStartIndex + i}", movi.ValorRetencion);
                                    cmd.Parameters.AddWithValue($"@Fenaice{batchStartIndex + i}", movi.Fenaice);
                                    cmd.Parameters.AddWithValue($"@ReteICA{batchStartIndex + i}", movi.ReteICA);
                                    cmd.Parameters.AddWithValue($"@IdRFte{batchStartIndex + i}", dictionary.TryGetValue(movi.IdRFte, out var rfte) ? cxp : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@RFte{batchStartIndex + i}", movi.RFte);

                                    cmd.Parameters.AddWithValue($"@Asohofrucol{batchStartIndex + i}", movi.Asohofrucol);
                                    cmd.Parameters.AddWithValue($"@FNFP{batchStartIndex + i}", movi.FNFP);
                                    cmd.Parameters.AddWithValue($"@Periodo{batchStartIndex + i}", movi.Periodo);
                                    cmd.Parameters.AddWithValue($"@Consecutivo{batchStartIndex + i}", i);

                                    cmd.Parameters.AddWithValue($"@IdUsuarioElaboro{batchStartIndex + i}",
                                    usuarios.TryGetValue(movi.IdUsuarioElaboro, out var userId) ? userId : (object)DBNull.Value);
    

                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{fact_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarModeloContable(List<t_modelContable> model_list, int batchSize = 2000)
        {
            try
            {
                if (model_list == null || !model_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();


                    //SELECT "Id", "Categoria"

                    // Procesar en lotes
                    while (contador < model_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""ModeloContable""(
                                 ""CodigoModelo"", 
                                 ""Nombre"", 
                                 ""Estado"", 
                                 ""CuentaGasto"", 
                                 ""CuentaDepreciacionAcumulada""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < model_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = model_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        
                                      @CodigoModelo{contador},
                                      @Nombre{contador}, 
                                      @Estado{contador}, 
                                      @CuentaGasto{contador},
                                      @CuentaDepreciacionAcumulada{contador}   
                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = model_list[batchStartIndex + i];

                                    if (movi == null) continue;


                                    cmd.Parameters.AddWithValue($"@CodigoModelo{batchStartIndex + i}", movi.CodigoModelo);
                                    cmd.Parameters.AddWithValue($"@Nombre{batchStartIndex + i}", movi.Nombre);
                                    cmd.Parameters.AddWithValue($"@Estado{batchStartIndex + i}", movi.Estado);
                                    cmd.Parameters.AddWithValue($"@CuentaGasto{batchStartIndex + i}", movi.CuentaGasto);
                                    cmd.Parameters.AddWithValue($"@CuentaDepreciacionAcumulada{batchStartIndex + i}", movi.CuentaDepreciacionAcumulada);
                   

                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();
                                await conn.CloseAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{model_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        } 

        public static async Task InsertarPlanCargue(List<t_planCargue> cargue_list, int batchSize = 2000)
        {
            try
            {
                if (cargue_list == null || !cargue_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();

                    // Diccionarios en memoria

                    var Conductor = new Dictionary<string, int>();
                    var Placa = new Dictionary<string, int>();


                    // Movimientos (IdDocumento + Numero)

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Documento"",""Id"" FROM public.""domiciliosconductores""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string Documento = reader.GetString(0).Trim();
                            int Id = reader.GetInt32(1);

                            Conductor[Documento] = Id;
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT id, placa FROM public.domiciliosvehiculos", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string placa = reader.GetString(1);

                            Placa[placa] = id;
                        }
                    }


                    //SELECT "Id", "Categoria"

                    // Procesar en lotes
                    while (contador < cargue_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""ProcesoPlanCargue""(
                                 ""IdPlanCargue"", 
                                 ""CantidadProducto"", 
                                 ""IdRuta"", 
                                 ""IdConductor"", 
                                 ""Observaciones"", 
                                 ""FechaSalida"", 
                                 ""IdPlaca""
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < cargue_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = cargue_list[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        
                                      @IdPlanCargue{contador},
                                      @CantidadProducto{contador},
                                      @IdRuta{contador},
                                      @IdConductor{contador},
                                      @Observaciones{contador},
                                      @FechaSalida{contador},
                                      @IdPlaca{contador}   

                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = cargue_list[batchStartIndex + i];

                                    
                                    if (movi == null) continue;


                                    cmd.Parameters.AddWithValue($"@IdPlanCargue{batchStartIndex + i}", Convert.ToInt32(movi.IdPlanCargue));
                                    cmd.Parameters.AddWithValue($"@CantidadProducto{batchStartIndex + i}", movi.CantidadProducto);
                                    cmd.Parameters.AddWithValue($"@IdRuta{batchStartIndex + i}", Convert.ToInt32(movi.IdRuta));


                                    if (Conductor.TryGetValue((movi.IdConductor), out int conductor))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdConductor{batchStartIndex + i}", conductor);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró conductor ({movi.IdConductor})");
                                        cmd.Parameters.AddWithValue($"@IdConductor{batchStartIndex + i}", DBNull.Value);
                                    }

                                    //cmd.Parameters.AddWithValue($"@IdCategoria{batchStartIndex + i}",
                                    // categoria.ContainsKey((movi.IdCategoria,movi.scat_ppe,movi.sbcat_ppe))
                                    //     ? categoria[(movi.IdCategoria, movi.scat_ppe, movi.sbcat_ppe)]
                                    //     : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue($"@Observaciones{batchStartIndex + i}", movi.Observaciones);


                                    if (movi.FechaSalida != null)
                                    {
                                        cmd.Parameters.AddWithValue($"@FechaSalida{batchStartIndex + i}", movi.FechaSalida);
                                    }
                                    else
                                    {
                                        cmd.Parameters.AddWithValue($"@FechaSalida{batchStartIndex + i}", DBNull.Value);
                                    }
                                    
                                        


                                   // cmd.Parameters.AddWithValue($"@FechaSalida{batchStartIndex + i}", movi.FechaSalida);


                                    if (Placa.TryGetValue((movi.IdPlaca), out int placa))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdPlaca{batchStartIndex + i}", placa);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró conductor ({movi.IdConductor})");
                                        cmd.Parameters.AddWithValue($"@IdPlaca{batchStartIndex + i}", DBNull.Value);
                                    }

                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();
                                await conn.CloseAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{cargue_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarSepDocumentos( int batchSize = 2000)
        {
            try
            {


                Migracion.Utilidades.Utilidades util = new Migracion.Utilidades.Utilidades();

                int contador = 0;
                var cronometer = Stopwatch.StartNew();
                var lista = new List<ResultadoJoin>();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();


                    var listaJoin = await Migracion.Utilidades.Utilidades.ListarSeparadoJoinAsync();
                    //SELECT "Id", "Categoria"

                    // Procesar en lotes
                    while (contador < listaJoin.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.separadomercanciadocumentos(
                                  separadomercanciaid, 
                                  cajasmovimientosid, 
                                  productoid, 
                                  nombreproducto                                 
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < listaJoin.Count && currentBatchSize < batchSize)
                                {
                                    var mov = listaJoin[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        
                                       @separadomercanciaid{contador},
                                       @cajasmovimientosid{contador},                                        
                                       @productoid{contador}, 
                                       @nombreproducto{contador}
                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = listaJoin[batchStartIndex + i];

                                    
                                    if (movi == null) continue;


                                    cmd.Parameters.AddWithValue($"@separadomercanciaid{batchStartIndex + i}", movi.separadomercanciaid);
                                    cmd.Parameters.AddWithValue($"@cajasmovimientosid{batchStartIndex + i}", movi.cajasmovimientosid);
                                    cmd.Parameters.AddWithValue($"@productoid{batchStartIndex + i}", movi.productoid);
                                    cmd.Parameters.AddWithValue($"@nombreproducto{batchStartIndex + i}", movi.nombreproducto ?? (object)DBNull.Value);

                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();
                                await conn.CloseAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{listaJoin.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarOrdenCompraSucursal(int batchSize = 2000)
        {
            try
            {


                Migracion.Utilidades.Utilidades util = new Migracion.Utilidades.Utilidades();

                int contador = 0;
                var cronometer = Stopwatch.StartNew();
                var lista = new List<ResultadoJoin>();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();


                    var listaJoin = await Migracion.Utilidades.Utilidades.ListarOrdenSucursalJoinAsync();
                    //SELECT "Id", "Categoria"

                    // Procesar en lotes
                    while (contador < listaJoin.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.ordenesdecomprasucursales(
                                  ordenesdecompraid, 
                                  sucursalid
                                ) VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < listaJoin.Count && currentBatchSize < batchSize)
                                {
                                    var mov = listaJoin[contador];
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"(
                                        
                                       @ordenesdecompraid{contador},
                                       @sucursalid{contador}  
                                    )";
                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros del lote
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = listaJoin[batchStartIndex + i];


                                    if (movi == null) continue;


                                    cmd.Parameters.AddWithValue($"@ordenesdecompraid{batchStartIndex + i}", movi.ordenesdecompraid);
                                    cmd.Parameters.AddWithValue($"@sucursalid{batchStartIndex + i}", movi.sucursalid);

                                }

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();
                                await conn.CloseAsync();

                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{listaJoin.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado en {tiempoTranscurrido.TotalSeconds:F2} segundos ({tiempoTranscurrido.TotalMinutes:F2} min)");
            }
            catch (Exception ex)
            {
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarMovdoc(List<t_mov_contables> movdoc_list, List<t_cuen> cuent_list, List<t_cabdoc> cabdocList, int batchSize = 3000)
        {
            try
            {
                // Validación inicial
                if (movdoc_list == null || !movdoc_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {



                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();



                    // se crean la variables de memoria 
                    var sucursales = new Dictionary<string, (int Id, string Nombre)>();
                    var terceros = new Dictionary<long, (long id, string RazonSocial)>();
                    var documentos = new Dictionary<string, int>();
                    var dictionary = new Dictionary<string, int>();
                    var usuarios = new Dictionary<string, string>();


                    //var mercaPesosPorCodigo  = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT codigo, id, nombre FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string codigo = reader.GetString(0);
                            int id = reader.GetInt32(1);
                            string nombre = reader.GetString(2);
                            sucursales[codigo] = (id, nombre);
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT ""RazonSocial"",""Identificacion"", ""Id"" FROM ""Terceros""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string RazonSocial = reader.GetString(0);
                            long identificacion = reader.GetInt64(1);
                            long id = reader.GetInt32(2);
                            terceros[identificacion] = (id,RazonSocial);
                        }
                    }


                    // Documentos
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM ""Documentos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            documentos[codigo] = id;
                        }
                    }


                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""CodCue"" FROM public.""PucMaestro""", conn))
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int id = reader.GetInt32(0);
                            string CodCue = reader.GetString(1);

                            dictionary[CodCue] = id;
                        }
                    }

                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""UserName"" FROM ""AspNetUsers""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string id = reader.GetString(0);       // Id en AspNetUsers casi siempre es string
                            string username = reader.GetString(1); // UserName
                            usuarios[username] = id;
                        }
                    }



                    while (contador < movdoc_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                            INSERT INTO public.""MovimientosContables""(                                
                                
                                ""Sucursal"", 
                                ""Documento"", 
                                ""Fecha"", 
                                ""Periodo"", 
                                ""Consecutivo"",
                                ""Tercero"", 
                                ""TerceroNombre"", 
                                ""CxPagar"", 
                                ""Zona"", 
                                ""Vendedor"", 
                                ""Observaciones"", 
                                ""Factura"", 
                                ""IdUsuario"", 
                                ""IdSucursal"", 
                                ""IdDocumento"", 
                                ""IdCxPagar"", 
                                ""IdTercero"", 
                                ""IdEstado"", 
                                ""IdCaja"", 
                                ""Concepto""
                                )VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < movdoc_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = movdoc_list[contador];
                                    string usuario = "";

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }



                                    var values = $@"
                                (
                                    
                                    
                                    @Sucursal{contador},
                                    @Documento{contador},
                                    @Fecha{contador},
                                    @Periodo{contador},
                                    @Consecutivo{contador},
                                    @Tercero{contador},
                                    @TerceroNombre{contador},
                                    @CxPagar{contador},
                                    @Zona{contador},
                                    @Vendedor{contador},
                                    @Observaciones{contador},
                                    @Factura{contador},
                                    @IdUsuario{contador},
                                    @IdSucursal{contador},
                                    @IdDocumento{contador},
                                    @IdCxPagar{contador},
                                    @IdTercero{contador},                                    
                                    @IdEstado{contador},
                                    @IdCaja{contador},
                                    @Concepto{contador}


                                )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }


                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                bool avisoMostrado = false;


                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var movi = movdoc_list[batchStartIndex + i];
                                    string usuario = "";
                                    string cuen_final = "";

                                    //if (i == 66)
                                    //{
                                    //    System.Diagnostics.Debugger.Break();
                                    //}

                                    var usu = cabdocList.FirstOrDefault(c => c.docum == movi.IdDocumento && c.numero == movi.Numero && c.consecut == movi.Periodo);

                                    if (usu != null)
                                    {
                                        usuario = usu.usuario;
                                    }

                                    var cuen = cuent_list.FirstOrDefault(c => c.cod_ret == movi.CxPagar);

                                    if (cuen != null)
                                    {
                                        cuen_final = cuen.cuenta;
                                    }

                                    // Validación adicional
                                    if (movi == null) continue;

                                    object idTerceroValue = DBNull.Value;
                                    string nombreTer = "";
                                    object cuenValue = DBNull.Value;


                                    if (movi.Tercero.HasValue && terceros != null)
                                    {
                                        if (terceros.TryGetValue(movi.Tercero.Value, out var idT))
                                        { 
                                            idTerceroValue = idT.id;
                                            nombreTer = idT.RazonSocial;
                                        }
                                        else if (!avisoMostrado)  // <-- solo la primera vez
                                        {
                                            Console.WriteLine($"⚠ No se encontró IdTercero {movi.Tercero.Value} en diccionario.");
                                            avisoMostrado = true;
                                        }

                                    }

                                    if (dictionary != null)
                                    {
                                        if (dictionary.TryGetValue(cuen_final , out var idcc))
                                        {
                                            cuenValue = idcc;
                                        }

                                    }


                                    cmd.Parameters.AddWithValue($"@Sucursal{batchStartIndex + i}", movi.Sucursal); 
                                    cmd.Parameters.AddWithValue($"@Documento{batchStartIndex + i}", movi.Documento);
                                    cmd.Parameters.AddWithValue($"@Fecha{batchStartIndex + i}", movi.Fecha); 
                                    cmd.Parameters.AddWithValue($"@Periodo{batchStartIndex + i}", movi.Periodo);
                                    cmd.Parameters.AddWithValue($"@Consecutivo{batchStartIndex + i}", i); // hacerlo contador@Consecutivo{ contador},
                                    cmd.Parameters.AddWithValue($"@Tercero{batchStartIndex + i}", movi.Tercero);
                                    cmd.Parameters.AddWithValue($"@TerceroNombre{batchStartIndex + i}", nombreTer);
                                    cmd.Parameters.AddWithValue($"@CxPagar{batchStartIndex + i}", cuen_final); 
                                    cmd.Parameters.AddWithValue($"@Zona{batchStartIndex + i}", movi.Zona); // hacerlo contador@Zona{ contador},
                                    cmd.Parameters.AddWithValue($"@Vendedor{batchStartIndex + i}", movi.Vendedor); // hacerlo contador@Vendedor{ contador},
                                    cmd.Parameters.AddWithValue($"@Observaciones{batchStartIndex + i}", movi.Observaciones); // hacerlo contador@Observaciones{ contador},
                                    cmd.Parameters.AddWithValue($"@Factura{batchStartIndex + i}", movi.Factura); // hacerlo contador@Factura{ contador},
                                    cmd.Parameters.AddWithValue($"@IdUsuario{batchStartIndex + i}",
                                    usuarios.TryGetValue(usuario, out var userId) ? userId : (object)DBNull.Value); 
                                    cmd.Parameters.AddWithValue($"@IdSucursal{batchStartIndex + i}",
                                    sucursales.ContainsKey(movi.Sucursal) ? sucursales[movi.Sucursal].Id : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@IdDocumento{batchStartIndex + i}", documentos.TryGetValue(movi.Documento, out var doc) ? doc : (object)DBNull.Value); 
                                    cmd.Parameters.AddWithValue($"@IdCxPagar{batchStartIndex + i}",cuenValue != null ? cuenValue : (object)DBNull.Value); 
                                    cmd.Parameters.AddWithValue($"@IdTercero{batchStartIndex + i}", idTerceroValue ?? DBNull.Value); 
                                    cmd.Parameters.AddWithValue($"@IdEstado{batchStartIndex + i}", 7);


                                    cmd.Parameters.AddWithValue($"@IdCaja{batchStartIndex + i}",
                                    string.IsNullOrWhiteSpace(movi.IdCaja)
                                        ? (object)DBNull.Value
                                        : Convert.ToInt64(movi.IdCaja));


                                    //cmd.Parameters.AddWithValue($"@IdCaja{batchStartIndex + i}", movi.IdCaja == "" ? (object)DBNull.Value : Convert.ToUInt64(movi.IdCaja));
                                    cmd.Parameters.AddWithValue($"@Concepto{batchStartIndex + i}", movi.Concepto);
                                    
                                    cmd.Parameters.AddWithValue($"@Numero{batchStartIndex + i}", movi.Numero);



                               
                                  //  cmd.Parameters.AddWithValue($"@IdUsuario{batchStartIndex + i}", usuario);
                                    


                                }


                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();


                                if (contador % 10000 == 0)
                                    Console.WriteLine($"Lote completado. Registros procesados: {contador}/{movdoc_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
            }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static void insertarSeparados(List<t_separados> separados_list, int batchSize = 100)
        {
            int contador = 0;
            int registrosInsertados = 0;
            int registrosOmitidos = 0;
            var cronometer = Stopwatch.StartNew();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                while (contador < separados_list.Count)
                {
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tx;

                        var commandText = new StringBuilder();
                        commandText.AppendLine(@"
                        INSERT INTO separadomercancia (
                        terceroid, fechayhoraconsumo, usuariocreaid, fechahoraregistro, cajasmovimientosid,
                        sucursalid)VALUES");

                        var valuesList = new List<string>();
                        var registrosValidosEnLote = new List<t_separados>();
                        int currentBatchSize = 0;

                        while (contador < separados_list.Count && currentBatchSize < batchSize)
                        {
                            var separados = separados_list[contador];

                            // Verificar si existe el registro en cajasmovimientos
                            using (var checkCmd = new NpgsqlCommand(
                                "SELECT COUNT(*) FROM public.cajasmovimientos WHERE numerofactura = @cajasmovimientosid",
                                conn, tx))
                            {
                                checkCmd.Parameters.AddWithValue("@cajasmovimientosid", separados.cajasmovimientosid.ToString());
                                var existe = Convert.ToInt32(checkCmd.ExecuteScalar()) > 0;

                                if (existe)
                                {
                                    registrosValidosEnLote.Add(separados);

                                    var values = $@"
                            (
                                (SELECT DISTINCT ""Id"" FROM ""Terceros"" WHERE ""Identificacion"" = @terceroid{registrosValidosEnLote.Count - 1} LIMIT 1),
                                @fechayhoraconsumo{registrosValidosEnLote.Count - 1},@usuariocreaid{registrosValidosEnLote.Count - 1},
                                @fechahoraregistro{registrosValidosEnLote.Count - 1},
                                (SELECT id FROM public.cajasmovimientos where numerofactura = @cajasmovimientosid{registrosValidosEnLote.Count - 1} and documentofactura =@documentofactura{registrosValidosEnLote.Count - 1}  LIMIT 1),
                                (SELECT DISTINCT id FROM ""Sucursales"" WHERE codigo = @sucursalid{registrosValidosEnLote.Count - 1})
                            )";

                                    valuesList.Add(values);
                                }
                                else
                                {
                                    Console.WriteLine($"⚠ Omitiendo registro: Número {separados.cajasmovimientosid} no existe en cajasmovimientos");
                                    registrosOmitidos++;
                                }
                            }

                            contador++;
                            currentBatchSize++;
                        }

                        // Solo ejecutar si hay registros válidos
                        if (registrosValidosEnLote.Count > 0)
                        {
                            commandText.Append(string.Join(",", valuesList));
                            cmd.CommandText = commandText.ToString();

                            // Agregar parámetros
                            for (int i = 0; i < registrosValidosEnLote.Count; i++)
                            {
                                var separados = registrosValidosEnLote[i];

                                cmd.Parameters.AddWithValue($"@terceroid{i}", separados.terceroid);
                                cmd.Parameters.AddWithValue($"@fechayhoraconsumo{i}", separados.fechayhoraconsumo);
                                cmd.Parameters.AddWithValue($"@usuariocreaid{i}", separados.usuariocreaid);
                                cmd.Parameters.AddWithValue($"@fechahoraregistro{i}", separados.fechahoraregistro);
                                cmd.Parameters.AddWithValue($"@cajasmovimientosid{i}", separados.cajasmovimientosid.ToString());
                                cmd.Parameters.AddWithValue($"@sucursalid{i}", separados.sucursalid);
                                cmd.Parameters.AddWithValue($"@documentofactura{i}", separados.documentofactura);
                            }

                            cmd.ExecuteNonQuery();
                            tx.Commit();
                            registrosInsertados += registrosValidosEnLote.Count;
                            Console.WriteLine($"✓ Lote insertado: {registrosValidosEnLote.Count} registros");
                        }
                        else
                        {
                            tx.Rollback();
                            Console.WriteLine("⚠ Lote omitido: no hay registros válidos");
                        }
                    }
                }
            }

            cronometer.Stop();
            Console.WriteLine($"\n=== RESUMEN ===");
            Console.WriteLine($"Registros insertados: {registrosInsertados}");
            Console.WriteLine($"Registros omitidos: {registrosOmitidos}");
            Console.WriteLine($"Total procesados: {separados_list.Count}");
            Console.WriteLine($"Tiempo: {cronometer.Elapsed.TotalSeconds:F2} segundos");
        }

        public static async Task InsertarCuotas(List<t_cuotas> cuotas_list, int batchSize = 100)
        {
            try
            {
                // Validación inicial
                if (cuotas_list == null || !cuotas_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Timeout=5;CommandTimeout=5"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)

                        try
                        {
                           await conn.OpenAsync();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error: {ex.Message}");
                        }

                    // cajasmovimientosid
                    var cajasmovimientos = new Dictionary<string, (int MovimientoId, string UsuarioId)>();

                    using (var cmd = new NpgsqlCommand(@"SELECT id, usuarioid, documentofactura, numerofactura FROM ""cajasmovimientos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string usuarioid = reader.GetString(1);
                            string documentofactura = reader.GetString(2);
                            string numerofactura = reader.GetString(3);
                            string clave = $"{documentofactura}|{numerofactura}";
                            cajasmovimientos[clave] = (id, usuarioid);
                        }
                    }

                    while (contador < cuotas_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.fenalpagcuotas(
                                fenalpagid, cuotanumero, valorcuota, fechavencimientocuota, fechahorapago, valorabono, cajasmovimientosid, idusuariopago
                                )VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < cuotas_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = cuotas_list[contador];

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"
                                (
                                    (SELECT id FROM fenalpag WHERE numerodocumento = @fenalpagid{contador} LIMIT 1), 
                                    @cuotanumero{contador}, 
                                    @valorcuota{contador},
                                    @fechavencimientocuota{contador},
                                    @fechahorapago{contador},
                                    @valorabono{contador},
                                    @cajasmovimientosid{contador},
                                    @idusuariopago{contador}
                                )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var cuotas = cuotas_list[batchStartIndex + i];

                                    // Validación adicional
                                    if (cuotas == null) continue;

                                    string clavecajamov = $"{cuotas.documentofactura}|{cuotas.cajasmovimientosid.ToString()}";

                                    if (cajasmovimientos.TryGetValue(clavecajamov, out var xcajamov))
                                    {
                                        cmd.Parameters.AddWithValue($"@cajasmovimientosid{batchStartIndex + i}", xcajamov.MovimientoId);
                                        cmd.Parameters.AddWithValue($"@idusuariopago{batchStartIndex + i}", xcajamov.UsuarioId);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"⚠️ No se encontró item para la clave ({clavecajamov})");
                                        cmd.Parameters.AddWithValue($"@cajasmovimientosid{batchStartIndex + i}", DBNull.Value);
                                        cmd.Parameters.AddWithValue($"@idusuariopago{batchStartIndex + i}", DBNull.Value);
                                    }

                                    cmd.Parameters.AddWithValue($"@fenalpagid{batchStartIndex + i}", cuotas.fenalpagid.ToString());
                                    cmd.Parameters.AddWithValue($"@cuotanumero{batchStartIndex + i}", int.Parse(cuotas.cuotanumero));
                                    cmd.Parameters.AddWithValue($"@valorcuota{batchStartIndex + i}", cuotas.valorcuota );
                                    cmd.Parameters.AddWithValue($"@fechavencimientocuota{batchStartIndex + i}", cuotas.fechavencimientocuota);
                                    cmd.Parameters.AddWithValue($"@fechahorapago{batchStartIndex + i}", cuotas.fechahorapago );
                                    cmd.Parameters.AddWithValue($"@valorabono{batchStartIndex + i}", cuotas.valorabono);
                                    //cmd.Parameters.AddWithValue($"@cajasmovimientosid{batchStartIndex + i}", cuotas.cajasmovimientosid.ToString());
                                    //cmd.Parameters.AddWithValue($"@idusuariopago{batchStartIndex + i}", cuotas.documentofactura);
                                }

                                Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                Console.WriteLine($"Lote completado. Registros procesados: {contador}/{cuotas_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
            }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarDetras(List<t_despacho> detras_list, int batchSize = 100)
        {
            try
            {
                // Validación inicial
                if (detras_list == null || !detras_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Timeout=5;CommandTimeout=5"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)

                        try
                        {
                            await conn.OpenAsync();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error: {ex.Message}");
                        }


                    // Diccionarios
                    var sucursales = new Dictionary<string, int>();     // codigo → id
                    var documentos = new Dictionary<string, int>();     // Codigo → Id
                    var productos  = new Dictionary<string, int>();      // Codigo → Id
                    var terceros   = new Dictionary<long, int>();
                    var usuarios   = new Dictionary<string, string>();    // UserName → Id (AspNetUsers suele tener Id string/Guid)
                    var bodegas    = new Dictionary<string, int>();        // codigo → id


                    // Sucursales
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM ""Sucursales""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            sucursales[codigo] = id;
                        }
                    }

                    // Documentos
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM ""Documentos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            documentos[codigo] = id;
                        }
                    }

                    // Productos
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""Codigo"" FROM ""Productos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            productos[codigo] = id;
                        }
                    }

                    // Terceros
                   
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Identificacion"", ""Id"" FROM ""Terceros""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            long identificacion = reader.GetInt64(0);
                            int id = reader.GetInt32(1);
                            terceros[identificacion] = id;
                        }
                    }


                    // Usuarios (AspNetUsers → OJO: normalmente el Id es GUID/string)
                    using (var cmd = new NpgsqlCommand(@"SELECT ""Id"", ""UserName"" FROM ""AspNetUsers""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string id = reader.GetString(0);       // Id en AspNetUsers casi siempre es string
                            string username = reader.GetString(1); // UserName
                            usuarios[username] = id;
                        }
                    }

                    // Bodegas
                    using (var cmd = new NpgsqlCommand(@"SELECT id, codigo FROM bodegas", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(0);
                            string codigo = reader.GetString(1);
                            bodegas[codigo] = id;
                        }
                    }



                    while (contador < detras_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.""MovimientoDespacho""(
                                ""IdSucursal"", ""IdDocumento"", ""Consecutivo"", ""Numero"", ""IdProducto"", ""CantidadReg"", ""CantidadEnt"", ""Fecha"", ""IdTercero"", ""Despacho"", 
                                ""TipoProceso"", ""Observacion"", ""DirecEntrega"", ""TelefEntrega"", ""IdSucursalTras"", ""DocumentoTras"", ""ConsecutivoTras"", ""NumeroTras"",
                                ""FechaDespacho"", ""IdUsuarioDespacho"", ""Estado"", ""FechaEn"", ""Estados"", ""IdJornada"", ""CodigoAut"", ""IdDomicilio"", ""TipoVenta"", ""Impreso"", 
                                ""Referencia"", ""Entrega"", ""Observacion2"", ""FechaEntrega"", ""IdZona"", ""IdSucursalDes"", ""Pendiente"", ""IdDocumentoSale"", ""IdBodegaDesp""                                
                                )VALUES");
                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < detras_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = detras_list[contador];

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"
                                (
                                    @IdSucursal{contador},
                                    @IdDocumento{contador} ,
                                    @Consecutivo{contador}, 
                                    @Numero{contador}, 
                                    @IdProducto{contador},
                                    @CantidadReg{contador}, 
                                    @CantidadEnt{contador},
                                    @Fecha{contador}, 
                                    @IdTercero{contador},
                                    @Despacho{contador},
                                    @TipoProceso{contador},
                                    @Observacion{contador},
                                    @DirecEntrega{contador}, 
                                    @TelefEntrega{contador},
                                    @IdSucursalTras{contador},
                                    @DocumentoTras{contador},
                                    @ConsecutivoTras{contador},
                                    @NumeroTras{contador}, 
                                    @FechaDespacho{contador}, 
                                    @IdUsuarioDespacho{contador}, 
                                    @Estado{contador}, 
                                    @FechaEn{contador}, 
                                    @Estados{contador},
                                    @IdJornada{contador},
                                    @CodigoAut{contador},
                                    @IdDomicilio{contador},
                                    @TipoVenta{contador},
                                    @Impreso{contador},
                                    @Referencia{contador},
                                    @Entrega{contador},
                                    @Observacion2{contador},
                                    @FechaEntrega{contador}, 
                                    @IdZona{contador}, 
                                    @IdSucursalDes{contador}, 
                                    @Pendiente{contador}, 
                                    @IdDocumentoSale{contador}, 
                                    @IdBodegaDesp{contador}


                                )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var detras = detras_list[batchStartIndex + i];

                                    int paramIndex = batchStartIndex + i;

                                    // Validación adicional
                                    if (detras == null) continue;
                                    Console.WriteLine($"[{paramIndex}] IdSucursal={detras.IdSucursal}, IdDocumento={detras.IdDocumento}, IdProducto={detras.IdProducto}, IdTercero={detras.IdTercero}, IdUsuarioDespacho={detras.IdUsuarioDespacho}, IdSucursalTras={detras.IdSucursalTras}, DocumentoTras={detras.DocumentoTras}, IdSucursalDes={detras.IdSucursalDes}, IdDocumentoSale={detras.IdDocumentoSale}, IdBodegaDesp={detras.IdBodegaDesp}");

                                    //if(i == 33)
                                    //{
                                    //    System.Diagnostics.Debugger.Break();
                                    //}



                                    //foreach (NpgsqlParameter p in cmd.Parameters)
                                    //{
                                    //    Console.WriteLine($"{p.ParameterName} = {p.Value} ({p.Value?.GetType()})");
                                    //}



                                    cmd.Parameters.AddWithValue(
                                         $"@Consecutivo{batchStartIndex + i}",
                                         string.IsNullOrWhiteSpace(detras.Consecutivo) ? (object)DBNull.Value : detras.Consecutivo
                                     );
                                    cmd.Parameters.AddWithValue(
                                         $"@Numero{batchStartIndex + i}",
                                         detras.Numero == 0 ? (object)DBNull.Value : detras.Numero
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@CantidadReg{batchStartIndex + i}", 
                                        detras.CantidadReg == 0 ? (object)DBNull.Value : detras.CantidadReg
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@CantidadEnt{batchStartIndex + i}", 
                                        detras.CantidadEnt == 0 ? (object)DBNull.Value : detras.CantidadEnt
                                    );
                                    cmd.Parameters.AddWithValue(
                                       $"@Fecha{batchStartIndex + i}",
                                       detras.Fecha == DateTime.MinValue ? (object)DBNull.Value : detras.Fecha
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@TipoProceso{batchStartIndex + i}",
                                         string.IsNullOrWhiteSpace(detras.TipoProceso) ? (object)DBNull.Value : detras.TipoProceso
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@ConsecutivoTras{batchStartIndex + i}",
                                        string.IsNullOrEmpty(detras.ConsecutivoTras) ? 0 : Convert.ToInt32(detras.ConsecutivoTras)
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@NumeroTras{batchStartIndex + i}", 
                                        detras.NumeroTras == 0 ? (object)DBNull.Value : detras.NumeroTras
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@FechaDespacho{batchStartIndex + i}", 
                                        detras.FechaDespacho == DateTime.MinValue ? (object)DBNull.Value : detras.FechaDespacho
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@Estado{batchStartIndex + i}",
                                        detras.Estado == 1 ? true : false
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@FechaEn{batchStartIndex + i}", 
                                        detras.FechaEn == DateTime.MinValue ? (object)DBNull.Value : detras.FechaEn
                                    );
                                   // cmd.Parameters.AddWithValue($"@Estados{batchStartIndex + i}", detras.Estados.ToString());
                                    cmd.Parameters.AddWithValue(
                                        $"@FechaEntrega{batchStartIndex + i}", 
                                        detras.FechaEntrega == DateTime.MinValue ? (object)DBNull.Value : detras.FechaEntrega
                                    );
                                    if (string.IsNullOrWhiteSpace(detras.IdZona))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdZona{batchStartIndex + i}", DBNull.Value);
                                    }
                                    else if (int.TryParse(detras.IdZona, out int idZonaInt))
                                    {
                                        cmd.Parameters.AddWithValue($"@IdZona{batchStartIndex + i}", idZonaInt);
                                    }
                                    else
                                    {
                                        // Si no es numérico válido
                                        cmd.Parameters.AddWithValue($"@IdZona{batchStartIndex + i}", DBNull.Value);

                                        // O, si quieres detener el proceso al detectar valores inválidos:
                                        // throw new FormatException($"El campo IdZona no es numérico: {detras.IdZona}");
                                    }
                                    cmd.Parameters.AddWithValue(
                                        $"@Pendiente{batchStartIndex + i}", 
                                        detras.Pendiente == 0 ? (object)DBNull.Value : detras.Pendiente
                                    );

                                    // Ejemplo de uso en tu ciclo con contador
                                    cmd.Parameters.AddWithValue($"@IdSucursal{batchStartIndex + i}", sucursales.TryGetValue(detras.IdSucursal, out var suc) ? suc : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@IdDocumento{batchStartIndex + i}", documentos.TryGetValue(detras.IdDocumento, out var doc) ? doc : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@IdProducto{batchStartIndex + i}", productos.TryGetValue(detras.IdProducto, out var prod) ? prod : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue(
                                        $"@IdTercero{batchStartIndex + i}",
                                        (detras.IdTercero.HasValue && terceros.TryGetValue(detras.IdTercero.Value, out var idT))
                                            ? (object)idT
                                            : DBNull.Value
                                    );
                                    cmd.Parameters.AddWithValue($"@IdSucursalTras{batchStartIndex + i}", sucursales.TryGetValue(detras.IdSucursalTras, out var sucTras) ? sucTras : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@DocumentoTras{batchStartIndex + i}", documentos.TryGetValue(detras.DocumentoTras, out var docTras) ? docTras : (object)DBNull.Value);
                                    // AspNetUsers → el Id normalmente es string/Guid
                                    cmd.Parameters.AddWithValue($"@IdUsuarioDespacho{batchStartIndex + i}", usuarios.TryGetValue(detras.IdUsuarioDespacho, out var user) ? user : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue($"@IdSucursalDes{batchStartIndex + i}", sucursales.TryGetValue(detras.IdSucursalDes, out var sucDes) ? sucDes : (object)DBNull.Value);
                                    cmd.Parameters.AddWithValue(
                                        $"@IdDocumentoSale{batchStartIndex + i}",
                                        string.IsNullOrEmpty(detras.IdDocumentoSale)
                                            ? (object)DBNull.Value
                                            : (documentos.TryGetValue(detras.IdDocumentoSale, out var docSale)
                                                ? docSale
                                                : (object)DBNull.Value)
                                    );
                                    cmd.Parameters.AddWithValue($"@IdBodegaDesp{batchStartIndex + i}", bodegas.TryGetValue(detras.IdBodegaDesp, out var bod) ? bod : (object)DBNull.Value);

                                    cmd.Parameters.AddWithValue(
                                        $"@Observacion{batchStartIndex + i}",
                                        string.IsNullOrEmpty(detras.Observacion) ? (object)DBNull.Value : detras.Observacion
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@Despacho{batchStartIndex + i}", 
                                        detras.Despacho == 0 ? (object)DBNull.Value : detras.Despacho
                                    ); 
                                    cmd.Parameters.AddWithValue($"@DirecEntrega{batchStartIndex + i}", string.IsNullOrEmpty(detras.DirecEntrega) ? (object)DBNull.Value : detras.DirecEntrega); 
                                    cmd.Parameters.AddWithValue($"@TelefEntrega{batchStartIndex + i}", string.IsNullOrEmpty(detras.TelefEntrega) ? (object)DBNull.Value : detras.TelefEntrega);
                                    cmd.Parameters.AddWithValue(
                                        $"@FechaEn{batchStartIndex + i}",
                                        detras.FechaEn == default ? (object)DBNull.Value : detras.FechaEn
                                    );
                                    cmd.Parameters.AddWithValue($"@Estados{batchStartIndex + i}", detras.Estados == 0 ? (object)DBNull.Value : detras.Estados); 
                                    cmd.Parameters.AddWithValue(
                                        $"@IdJornada{batchStartIndex + i}",
                                        detras.IdJornada == 0 ? (object)DBNull.Value : detras.IdJornada
                                    ); 
                                    cmd.Parameters.AddWithValue(
                                        $"@CodigoAut{batchStartIndex + i}", 
                                        detras.CodigoAut == 0 ? (object)DBNull.Value : detras.CodigoAut
                                    ); 
                                    cmd.Parameters.AddWithValue(
                                        $"@IdDomicilio{batchStartIndex + i}", 
                                        detras.IdDomicilio == 0 ? (object)DBNull.Value : detras.IdDomicilio
                                    );
                                    if (string.IsNullOrWhiteSpace(detras.TipoVenta))
                                    {
                                        cmd.Parameters.AddWithValue($"@TipoVenta{batchStartIndex + i}", DBNull.Value);
                                    }
                                    else if (int.TryParse(detras.TipoVenta, out int tipoVentaInt))
                                    {
                                        cmd.Parameters.AddWithValue($"@TipoVenta{batchStartIndex + i}", tipoVentaInt);
                                    }
                                    else
                                    {
                                        // Si quieres que los valores no numéricos caigan como NULL
                                        cmd.Parameters.AddWithValue($"@TipoVenta{batchStartIndex + i}", DBNull.Value);

                                        // O si prefieres fallar para detectar errores en los datos:
                                        // throw new FormatException($"TipoVenta no es numérico: {detras.TipoVenta}");
                                    }
                                    cmd.Parameters.AddWithValue(
                                        $"@Impreso{batchStartIndex + i}",
                                        detras.Impreso == 0 ? (object)DBNull.Value : detras.Impreso
                                    ); 
                                    cmd.Parameters.AddWithValue(
                                        $"@Referencia{batchStartIndex + i}", 
                                        string.IsNullOrEmpty(detras.Referencia) ? (object)DBNull.Value : detras.Referencia
                                    );
                                    cmd.Parameters.AddWithValue(
                                        $"@Entrega{batchStartIndex + i}",
                                        detras.Entrega == 0 ? (object)DBNull.Value : detras.Entrega
                                    ); 
                                    cmd.Parameters.AddWithValue(
                                        $"@Observacion2{batchStartIndex + i}",
                                        string.IsNullOrEmpty(detras.Observacion2) ? (object)DBNull.Value : detras.Observacion2
                                    ); 
                                    

                                }

                                Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");


                                foreach (NpgsqlParameter p in cmd.Parameters)
                                {
                                    Console.WriteLine($"{p.ParameterName} = {p.Value} ({p.Value?.GetType()})");
                                }


                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                Console.WriteLine($"Lote completado. Registros procesados: {contador}/{detras_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                 Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
            }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarCredit(List<t_cred2> cred_list, int batchSize = 100)
        {
            try
            {
                // Validación inicial
                if (cred_list == null || !cred_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Timeout=5;CommandTimeout=5"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)

                        try
                        {
                            await conn.OpenAsync();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error: {ex.Message}");
                        }


                    while (contador < cred_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.creditos(
	                            documentoreferencia, fechahoracredito, sucursalid, numerocuenta, documentoaf, tipoabono, totalcredito, consecutivoaf, aprobado, cuotafija, amortizado
                                )VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < cred_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = cred_list[contador];

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"
                                (
                                    @documentoreferencia{contador},
                                    @fechahoracredito{contador},
                                    (SELECT DISTINCT id FROM ""Sucursales"" WHERE codigo = @sucursalid{contador}),
                                    @numerocuenta{contador},
                                    @documentoaf{contador},
                                    @tipoabono{contador},
                                    @totalcredito{contador},
                                    @consecutivoaf{contador},
                                    @aprobado{contador},
                                    @cuotafija{contador},
                                    @amortizado{contador}

                                )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var cuotas = cred_list[batchStartIndex + i];

                                    // Validación adicional
                                    if (cuotas == null) continue;

                                    cmd.Parameters.AddWithValue($"@documentoreferencia{batchStartIndex + i}", cuotas.documentoreferencia);
                                    cmd.Parameters.AddWithValue($"@fechahoracredito{batchStartIndex + i}", cuotas.fechahoracredito);
                                    cmd.Parameters.AddWithValue($"@sucursalid{batchStartIndex + i}", cuotas.sucursalid);
                                    cmd.Parameters.AddWithValue($"@numerocuenta{batchStartIndex + i}", cuotas.numerocuenta);
                                    cmd.Parameters.AddWithValue($"@documentoaf{batchStartIndex + i}", cuotas.documentoaf);
                                    cmd.Parameters.AddWithValue($"@tipoabono{batchStartIndex + i}", cuotas.tipoabono);
                                    cmd.Parameters.AddWithValue($"@totalcredito{batchStartIndex + i}", cuotas.totalcredito);
                                    cmd.Parameters.AddWithValue($"@consecutivoaf{batchStartIndex + i}", cuotas.consecutivoaf);
                                    cmd.Parameters.AddWithValue($"@aprobado{batchStartIndex + i}", true);
                                    cmd.Parameters.AddWithValue($"@cuotafija{batchStartIndex + i}", true);
                                    cmd.Parameters.AddWithValue($"@amortizado{batchStartIndex + i}", cuotas.amortizado == "S" ? true : false );


                                }

                                Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                Console.WriteLine($"Lote completado. Registros procesados: {contador}/{cred_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
            }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarCuota2(List<t_credCuotas> cCuota_list, int batchSize = 100)
        {
            try
            {
                // Validación inicial
                if (cCuota_list == null || !cCuota_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Timeout=5;CommandTimeout=5"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)

                        try
                        {
                            await conn.OpenAsync();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error: {ex.Message}");
                        }


                    while (contador < cCuota_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.creditoscuotas(
	                            creditosid, numerocuota, valorcuota, pagada
                                )VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < cCuota_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = cCuota_list[contador];

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"
                                (
                                    
                                    (SELECT id FROM public.creditos where documentoreferencia= @creditosid{contador}),
                                    @numerocuota{contador},
                                    @valorcuota{contador},
                                    @pagada{contador}

                                )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var cuotas = cCuota_list[batchStartIndex + i];

                                    // Validación adicional
                                    if (cuotas == null) continue;

                                    cmd.Parameters.AddWithValue($"@creditosid{batchStartIndex + i}", cuotas.creditosid);
                                    cmd.Parameters.AddWithValue($"@numerocuota{batchStartIndex + i}", Int32.Parse(cuotas.numerocuota));
                                    cmd.Parameters.AddWithValue($"@valorcuota{batchStartIndex + i}", cuotas.valorcuota);
                                    cmd.Parameters.AddWithValue($"@pagada{batchStartIndex + i}", cuotas.pagada == "X" ? true : false);
                                    

                                }

                                Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                Console.WriteLine($"Lote completado. Registros procesados: {contador}/{cCuota_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
            }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        public static async Task InsertarDespacho(List<t_credCuotas> cCuota_list, int batchSize = 100)
        {
            try
            {
                // Validación inicial
                if (cCuota_list == null || !cCuota_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;Timeout=5;CommandTimeout=5"))
                {
                    if (conn.State != System.Data.ConnectionState.Open)

                        try
                        {
                            await conn.OpenAsync();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error: {ex.Message}");
                        }


                    while (contador < cCuota_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {

                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                                INSERT INTO public.creditoscuotas(
	                            ""IdSucursal"",        ""IdDocumento"",  ""Consecutivo"",  ""Numero"",       ""IdProducto"",     ""CantidadReg"",     ""CantidadEnt"",     ""Fecha"",      ""IdTercero"",
                                ""TipoProceso"",       ""IdSucursalTras"", ""DocumentoTras"",   ""ConsecutivoTras"", ""NumeroTras"", ""FechaDespacho"", 
                                ""IdUsuarioDespacho"", ""Estado"",       ""FechaEn"",      ""Estados"",      ""TipoVenta"",  
                                ""FechaEntrega"", ""IdSucursalDes"",""Pendiente"",      ""IdDocumentoSale"", ""IdBodegaDesp""
                                )VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < cCuota_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = cCuota_list[contador];

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"
                                (
                                    
                                    (SELECT id FROM public.creditos where documentoreferencia= @creditosid{contador}),
                                    @numerocuota{contador},
                                    @valorcuota{contador},
                                    @pagada{contador}

                                )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var cuotas = cCuota_list[batchStartIndex + i];

                                    // Validación adicional
                                    if (cuotas == null) continue;

                                    cmd.Parameters.AddWithValue($"@creditosid{batchStartIndex + i}", cuotas.creditosid);
                                    cmd.Parameters.AddWithValue($"@numerocuota{batchStartIndex + i}", Int32.Parse(cuotas.numerocuota));
                                    cmd.Parameters.AddWithValue($"@valorcuota{batchStartIndex + i}", cuotas.valorcuota);
                                    cmd.Parameters.AddWithValue($"@pagada{batchStartIndex + i}", cuotas.pagada == "X" ? true : false);


                                }

                                Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                Console.WriteLine($"Lote completado. Registros procesados: {contador}/{cCuota_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
            }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        //public static void insertarDataf(List<t_dataf> dataf_list, int batchSize = 100)
        //{
        //    int contador = 0;
        //    int registrosInsertados = 0;
        //    int registrosOmitidos = 0;
        //    var cronometer = Stopwatch.StartNew();

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        while (contador < dataf_list.Count)
        //        {
        //            using (var tx = conn.BeginTransaction())
        //            using (var cmd = new NpgsqlCommand())
        //            {
        //                cmd.Connection = conn;
        //                cmd.Transaction = tx;

        //                var commandText = new StringBuilder();
        //                commandText.AppendLine(@"

        //                INSERT INTO public.cajasmovimientosdatafono(
        //                 cajasmovimientosid,fechayhorasolicitud,solicitudoperacion,solicitudvalor,solicitudmonto,solicitudiva,solicitudcodigocajero,
        //                 respuestarespuesta,respuestaautorizacion,respuestatarjeta,respuestatipotarjeta,respuestafranquicia,respuestamonto,respuestaiva,
        //                 respuestarecibo,respuestacuotas,respuestarrn,respuestaobservacion)VALUES");

        //                var valuesList = new List<string>();
        //                var registrosValidosEnLote = new List<t_dataf>();
        //                int currentBatchSize = 0;

        //                while (contador < dataf_list.Count && currentBatchSize < batchSize)
        //                {
        //                    var dataf = dataf_list[contador];

        //                    // Verificar si existe el registro en cajasmovimientos
        //                    using (var checkCmd = new NpgsqlCommand(
        //                        "SELECT COUNT(*) FROM public.cajasmovimientos WHERE numerofactura = @cajasmovimientosid",
        //                        conn, tx))
        //                    {
        //                        checkCmd.Parameters.AddWithValue("@cajasmovimientosid", dataf.cajasmovimientosid.ToString());
        //                        var existe = Convert.ToInt32(checkCmd.ExecuteScalar()) > 0;

        //                        if (existe)
        //                        {
        //                            registrosValidosEnLote.Add(dataf);

        //                            var values = $@"
        //                    (
        //                        (SELECT id FROM public.cajasmovimientos where numerofactura = @cajasmovimientosid{registrosValidosEnLote.Count - 1} LIMIT 1),
        //                        @fechayhorasolicitud{registrosValidosEnLote.Count - 1},@solicitudoperacion{registrosValidosEnLote.Count - 1},@solicitudvalor{registrosValidosEnLote.Count - 1},@solicitudmonto{registrosValidosEnLote.Count - 1},@solicitudiva{registrosValidosEnLote.Count - 1},@solicitudcodigocajero{registrosValidosEnLote.Count - 1},
        //                        @respuestarespuesta{registrosValidosEnLote.Count - 1},@respuestaautorizacion{registrosValidosEnLote.Count - 1},@respuestatarjeta{registrosValidosEnLote.Count - 1},@respuestatipotarjeta{registrosValidosEnLote.Count - 1},@respuestafranquicia{registrosValidosEnLote.Count - 1},@respuestamonto{registrosValidosEnLote.Count - 1},@respuestaiva{registrosValidosEnLote.Count - 1},
        //                        @respuestarecibo{registrosValidosEnLote.Count - 1},@respuestacuotas{registrosValidosEnLote.Count - 1},@respuestarrn{registrosValidosEnLote.Count - 1},
        //                        @respuestaobservacion{registrosValidosEnLote.Count - 1}
        //                    )";

        //                            valuesList.Add(values);
        //                        }
        //                        else
        //                        {
        //                            Console.WriteLine($"⚠ Omitiendo registro: Número {dataf.cajasmovimientosid} no existe en cajasmovimientos");
        //                            registrosOmitidos++;
        //                        }
        //                    }

        //                    contador++;
        //                    currentBatchSize++;
        //                }

        //                // Solo ejecutar si hay registros válidos
        //                if (registrosValidosEnLote.Count > 0)
        //                {
        //                    commandText.Append(string.Join(",", valuesList));
        //                    cmd.CommandText = commandText.ToString();

        //                    // Agregar parámetros
        //                    for (int i = 0; i < registrosValidosEnLote.Count; i++)
        //                    {
        //                        int respuestaRecibo = 0;
        //                        int respuestaRrn = 0;
        //                        var dataf = registrosValidosEnLote[i];

        //                        if (string.IsNullOrEmpty(dataf.respuestarecibo))
        //                        {
        //                            Console.WriteLine($"⚠ Registro {i}: respuestarecibo está vacío, usando valor 0");
        //                            respuestaRecibo = 0;
        //                        }
        //                        else if (!int.TryParse(dataf.respuestarecibo, out respuestaRecibo))
        //                        {
        //                            Console.WriteLine($"⚠ Registro {i}: respuestarecibo '{dataf.respuestarecibo}' no es un número válido, usando valor 0");
        //                            respuestaRecibo = 0;
        //                        }
        //                        else
        //                        {
        //                            Console.WriteLine($"✓ Registro {i}: respuestarecibo = {respuestaRecibo}");
        //                        }


        //                        if (string.IsNullOrEmpty(dataf.respuestarrn))
        //                        {
        //                            Console.WriteLine($"⚠ Registro {i}: respuestarecibo está vacío, usando valor 0");
        //                            respuestaRrn = 0;
        //                        }
        //                        else if (!int.TryParse(dataf.respuestarrn, out respuestaRrn))
        //                        {
        //                            Console.WriteLine($"⚠ Registro {i}: respuestarecibo '{dataf.respuestarrn}' no es un número válido, usando valor 0");
        //                            respuestaRrn = 0;
        //                        }
        //                        else
        //                        {
        //                            Console.WriteLine($"✓ Registro {i}: respuestarecibo = {respuestaRrn}");
        //                        }




        //                        cmd.Parameters.AddWithValue($"@cajasmovimientosid{i}", dataf.cajasmovimientosid.ToString());
        //                        cmd.Parameters.AddWithValue($"@fechayhorasolicitud{i}", dataf.fechayhorasolicitud);
        //                        cmd.Parameters.AddWithValue($"@solicitudoperacion{i}", dataf.solicitudoperacion); 
        //                        cmd.Parameters.AddWithValue($"@solicitudvalor{i}", dataf.solicitudvalor);
        //                        cmd.Parameters.AddWithValue($"@solicitudmonto{i}", dataf.solicitudmonto); 
        //                        cmd.Parameters.AddWithValue($"@solicitudiva{i}", dataf.solicitudiva);  
        //                        cmd.Parameters.AddWithValue($"@solicitudcodigocajero{i}", dataf.solicitudcodigocajero); 
        //                     cmd.Parameters.AddWithValue($"@respuestarespuesta{i}", dataf.respuestarespuesta); 
        //                        cmd.Parameters.AddWithValue($"@respuestaautorizacion{i}", dataf.respuestaautorizacion); 
        //                        cmd.Parameters.AddWithValue($"@respuestatarjeta{i}", dataf.respuestatarjeta); 
        //                        cmd.Parameters.AddWithValue($"@respuestatipotarjeta{i}", dataf.respuestatipotarjeta); 
        //                        cmd.Parameters.AddWithValue($"@respuestafranquicia{i}", dataf.respuestafranquicia); 
        //                        cmd.Parameters.AddWithValue($"@respuestamonto{i}", dataf.respuestamonto); 
        //                        cmd.Parameters.AddWithValue($"@respuestaiva{i}", dataf.respuestaiva); 
        //                     cmd.Parameters.AddWithValue($"@respuestarecibo{i}", respuestaRecibo); 
        //                        cmd.Parameters.AddWithValue($"@respuestacuotas{i}", dataf.respuestacuotas); 
        //                        cmd.Parameters.AddWithValue($"@respuestarrn{i}", respuestaRrn); 
        //                        cmd.Parameters.AddWithValue($"@respuestaobservacion{i}", dataf.respuestaobservacion); 

        //                    }

        //                     cmd.ExecuteNonQuery();
        //                    tx.Commit();
        //                    registrosInsertados += registrosValidosEnLote.Count;
        //                    Console.WriteLine($"✓ Lote insertado: {registrosValidosEnLote.Count} registros");
        //                }
        //                else
        //                {
        //                    tx.Rollback();
        //                    Console.WriteLine("⚠ Lote omitido: no hay registros válidos");
        //                }
        //            }
        //        }
        //    }

        //    cronometer.Stop();
        //    Console.WriteLine($"\n=== RESUMEN ===");
        //    Console.WriteLine($"Registros insertados: {registrosInsertados}");
        //    Console.WriteLine($"Registros omitidos: {registrosOmitidos}");
        //    Console.WriteLine($"Total procesados: {dataf_list.Count}");
        //    Console.WriteLine($"Tiempo: {cronometer.Elapsed.TotalSeconds:F2} segundos");
        //}

        public static async Task insertarDataf(List<t_dataf> dataf_list, int batchSize = 100)
        {
            try
            {
                // Validación inicial
                if (dataf_list == null || !dataf_list.Any())
                {
                    Console.WriteLine("La lista de movimientos está vacía o es null");
                    return;
                }

                int contador = 0;
                var cronometer = Stopwatch.StartNew();

                using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
                {



                    if (conn.State != System.Data.ConnectionState.Open)
                        await conn.OpenAsync();



                    // se crean la variables de memoria 



                    var cajasMovimientos = new Dictionary<string, int>();


                    //var mercaPesosPorCodigo  = new Dictionary<string, int>();

                    using (var cmd = new NpgsqlCommand(@"SELECT numerofactura, id FROM public.""cajasmovimientos""", conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string numeroFactura = reader.GetString(0).Trim();
                            int id = reader.GetInt32(1);

                            if (!cajasMovimientos.ContainsKey(numeroFactura))
                                cajasMovimientos[numeroFactura] = id;
                        }
                    }


                    while (contador < dataf_list.Count)
                    {
                        using (var tx = await conn.BeginTransactionAsync())
                        using (var cmd = new NpgsqlCommand())
                        {
                            try
                            {
                                cmd.Connection = conn;
                                cmd.Transaction = tx;

                                var commandText = new StringBuilder();
                                commandText.AppendLine(@"
                    INSERT INTO cajasmovimientosdatafono (
                    cajasmovimientosid,fechayhorasolicitud,solicitudoperacion,solicitudvalor,solicitudmonto,solicitudiva,solicitudcodigocajero,
                    respuestarespuesta,respuestaautorizacion,respuestatarjeta,respuestatipotarjeta,respuestafranquicia,respuestamonto,respuestaiva,
                    respuestarecibo,respuestacuotas,respuestarrn,respuestaobservacion)VALUES");

                                var valuesList = new List<string>();
                                int currentBatchSize = 0;
                                int batchStartIndex = contador;

                                while (contador < dataf_list.Count && currentBatchSize < batchSize)
                                {
                                    var mov = dataf_list[contador];

                                    // Validar que mov no sea null
                                    if (mov == null)
                                    {
                                        Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
                                        contador++;
                                        continue;
                                    }

                                    var values = $@"
                        (
                            @cajasmovimientosid{contador} ,
                            @fechayhorasolicitud{contador},@solicitudoperacion{contador},@solicitudvalor{contador},@solicitudmonto{contador},@solicitudiva{contador},@solicitudcodigocajero{contador},
                            @respuestarespuesta{contador},@respuestaautorizacion{contador},@respuestatarjeta{contador},@respuestatipotarjeta{contador},@respuestafranquicia{contador},@respuestamonto{contador},@respuestaiva{contador},
                            @respuestarecibo{contador},@respuestacuotas{contador},@respuestarrn{contador},
                            @respuestaobservacion{contador}
                            

                        )";

                                    valuesList.Add(values);
                                    contador++;
                                    currentBatchSize++;
                                }

                                int current = 0;

                                current = currentBatchSize;

                                if (valuesList.Count == 0)
                                {
                                    Console.WriteLine("No hay valores válidos para insertar en este lote");
                                    break;
                                }

                                commandText.Append(string.Join(",", valuesList));
                                cmd.CommandText = commandText.ToString();

                                // Agregar parámetros para el lote actual
                                for (int i = 0; i < currentBatchSize; i++)
                                {
                                    var dataf = dataf_list[batchStartIndex + i];

                                    //System.Diagnostics.Debugger.Break();
                                    //if (i == 10)
                                    //{
                                    //    System.Diagnostics.Debugger.Break();
                                    //}


                                    // Validación adicional
                                    if (dataf == null) continue;

                                    int respuestaRecibo = 0;
                                    int respuestaRrn = 0;
                                    

                                    if (string.IsNullOrEmpty(dataf.respuestarecibo))
                                    {
                                        Console.WriteLine($"⚠ Registro {i}: respuestarecibo está vacío, usando valor 0");
                                        respuestaRecibo = 0;
                                    }
                                    else if (!int.TryParse(dataf.respuestarecibo, out respuestaRecibo))
                                    {
                                        Console.WriteLine($"⚠ Registro {i}: respuestarecibo '{dataf.respuestarecibo}' no es un número válido, usando valor 0");
                                        respuestaRecibo = 0;
                                    }
                                    else
                                    {
                                        Console.WriteLine($"✓ Registro {i}: respuestarecibo = {respuestaRecibo}");
                                    }


                                    if (string.IsNullOrEmpty(dataf.respuestarrn))
                                    {
                                        Console.WriteLine($"⚠ Registro {i}: respuestarecibo está vacío, usando valor 0");
                                        respuestaRrn = 0;
                                    }
                                    else if (!int.TryParse(dataf.respuestarrn, out respuestaRrn))
                                    {
                                        Console.WriteLine($"⚠ Registro {i}: respuestarecibo '{dataf.respuestarrn}' no es un número válido, usando valor 0");
                                        respuestaRrn = 0;
                                    }
                                    else
                                    {
                                        Console.WriteLine($"✓ Registro {i}: respuestarecibo = {respuestaRrn}");
                                    }



                                    cmd.Parameters.AddWithValue(
                                        $"@cajasmovimientosid{batchStartIndex + i}",
                                        (dataf.cajasmovimientosid != 0 &&
                                         cajasMovimientos.TryGetValue(dataf.cajasmovimientosid.ToString(), out var idCaja))
                                            ? (object)idCaja
                                            : DBNull.Value
                                    );


                                    cmd.Parameters.AddWithValue($"@fechayhorasolicitud{batchStartIndex + i}", dataf.fechayhorasolicitud);
                                    cmd.Parameters.AddWithValue($"@solicitudoperacion{batchStartIndex + i}", dataf.solicitudoperacion);
                                    cmd.Parameters.AddWithValue($"@solicitudvalor{batchStartIndex + i}", dataf.solicitudvalor);
                                    cmd.Parameters.AddWithValue($"@solicitudmonto{batchStartIndex + i}", dataf.solicitudmonto);
                                    cmd.Parameters.AddWithValue($"@solicitudiva{batchStartIndex + i}", dataf.solicitudiva);
                                    cmd.Parameters.AddWithValue($"@solicitudcodigocajero{batchStartIndex + i}", dataf.solicitudcodigocajero);
                                    cmd.Parameters.AddWithValue($"@respuestarespuesta{batchStartIndex + i}", dataf.respuestarespuesta);
                                    cmd.Parameters.AddWithValue($"@respuestaautorizacion{batchStartIndex + i}", dataf.respuestaautorizacion);
                                    cmd.Parameters.AddWithValue($"@respuestatarjeta{batchStartIndex + i}", dataf.respuestatarjeta);
                                    cmd.Parameters.AddWithValue($"@respuestatipotarjeta{batchStartIndex + i}", dataf.respuestatipotarjeta);
                                    cmd.Parameters.AddWithValue($"@respuestafranquicia{batchStartIndex + i}", dataf.respuestafranquicia);
                                    cmd.Parameters.AddWithValue($"@respuestamonto{batchStartIndex + i}", dataf.respuestamonto);
                                    cmd.Parameters.AddWithValue($"@respuestaiva{batchStartIndex + i}", dataf.respuestaiva);
                                    cmd.Parameters.AddWithValue($"@respuestarecibo{batchStartIndex + i}", respuestaRecibo);
                                    cmd.Parameters.AddWithValue($"@respuestacuotas{batchStartIndex + i}", dataf.respuestacuotas);
                                    cmd.Parameters.AddWithValue($"@respuestarrn{batchStartIndex + i}", respuestaRrn);
                                    cmd.Parameters.AddWithValue($"@respuestaobservacion{batchStartIndex + i}", dataf.respuestaobservacion);


                                }

                                Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

                                await cmd.ExecuteNonQueryAsync();
                                await tx.CommitAsync();

                                Console.WriteLine($"Lote completado. Registros procesados: {contador}/{dataf_list.Count}");
                            }
                            catch (Exception batchEx)
                            {
                                Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
                                await tx.RollbackAsync();

                                // Opcional: continuar con el siguiente lote o terminar
                                // throw; // Descomenta si quieres que termine en el primer error
                            }
                        }
                    }
                }

                cronometer.Stop();
                TimeSpan tiempoTranscurrido = cronometer.Elapsed;
                Console.WriteLine($"Proceso completado:");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
                Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
            }
            catch (Exception ex)
            {
                // ✅ Manejo seguro de excepciones
                Console.WriteLine("=== ERROR GENERAL ===");
                Console.WriteLine($"Mensaje: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
                }

                Console.WriteLine($"Stack Trace: {ex.StackTrace}");

                // Log adicional para debugging
                Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
            }
        }

        ////public static async Task insertarFpago(List<t_dataf> dataf_list, int batchSize = 100)
        //{
        //    try
        //    {
        //        // Validación inicial
        //        if (dataf_list == null || !dataf_list.Any())
        //        {
        //            Console.WriteLine("La lista de movimientos está vacía o es null");
        //            return;
        //        }

        //        int contador = 0;
        //        var cronometer = Stopwatch.StartNew();

        //        using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro;"))
        //        {



        //            if (conn.State != System.Data.ConnectionState.Open)
        //                await conn.OpenAsync();



        //            // se crean la variables de memoria 



        //            var cajasMovimientos = new Dictionary<string, int>();


        //            //var mercaPesosPorCodigo  = new Dictionary<string, int>();

        //            using (var cmd = new NpgsqlCommand(@"SELECT numerofactura, id FROM public.""cajasmovimientos""", conn))
        //            using (var reader = await cmd.ExecuteReaderAsync())
        //            {
        //                while (await reader.ReadAsync())
        //                {
        //                    string numeroFactura = reader.GetString(0).Trim();
        //                    int id = reader.GetInt32(1);

        //                    if (!cajasMovimientos.ContainsKey(numeroFactura))
        //                        cajasMovimientos[numeroFactura] = id;
        //                }
        //            }


        //            while (contador < dataf_list.Count)
        //            {
        //                using (var tx = await conn.BeginTransactionAsync())
        //                using (var cmd = new NpgsqlCommand())
        //                {
        //                    try
        //                    {
        //                        cmd.Connection = conn;  
        //                        cmd.Transaction = tx;

        //                        var commandText = new StringBuilder();
        //                        commandText.AppendLine(@"
        //                        INSERT INTO cajasmovimientosformasdepago (
        //                        cajasmovimientosid,fechayhorasolicitud,solicitudoperacion,solicitudvalor,solicitudmonto,solicitudiva,solicitudcodigocajero,
        //                        respuestarespuesta,respuestaautorizacion,respuestatarjeta,respuestatipotarjeta,respuestafranquicia,respuestamonto,respuestaiva,
        //                        respuestarecibo,respuestacuotas,respuestarrn,respuestaobservacion)VALUES");

        //                        var valuesList = new List<string>();
        //                        int currentBatchSize = 0;
        //                        int batchStartIndex = contador;

        //                        while (contador < dataf_list.Count && currentBatchSize < batchSize)
        //                        {
        //                            var mov = dataf_list[contador];

        //                            // Validar que mov no sea null
        //                            if (mov == null)
        //                            {
        //                                Console.WriteLine($"Movimiento en índice {contador} es null, saltando...");
        //                                contador++;
        //                                continue;
        //                            }

        //                            var values = $@"
        //                (

        //                    @cajasmovimientosid{contador},
        //                    @fechayhorasolicitud{contador},@solicitudoperacion{contador},@solicitudvalor{contador},@solicitudmonto{contador},@solicitudiva{contador},
        //                    @solicitudcodigocajero{contador},@respuestarespuesta{contador},@respuestaautorizacion{contador},@respuestatarjeta{contador},
        //                    @respuestatipotarjeta{contador},@respuestafranquicia{contador},@respuestamonto{contador},@respuestaiva{contador},
        //                    @respuestarecibo{contador},@respuestacuotas{contador},@respuestarrn{contador},
        //                    @respuestaobservacion{contador}


        //                )";

        //                            valuesList.Add(values);
        //                            contador++;
        //                            currentBatchSize++;
        //                        }

        //                        int current = 0;

        //                        current = currentBatchSize;

        //                        if (valuesList.Count == 0)
        //                        {
        //                            Console.WriteLine("No hay valores válidos para insertar en este lote");
        //                            break;
        //                        }

        //                        commandText.Append(string.Join(",", valuesList));
        //                        cmd.CommandText = commandText.ToString();

        //                        // Agregar parámetros para el lote actual
        //                        for (int i = 0; i < currentBatchSize; i++)
        //                        {
        //                            //var mercap = fpago_list[batchStartIndex + i];

        //                            //System.Diagnostics.Debugger.Break();
        //                            //if (i == 10)
        //                            //{
        //                            //    System.Diagnostics.Debugger.Break();
        //                            //}


        //                            // Validación adicional
        //                            if (mercap == null) continue;

        //                            //cmd.Parameters.AddWithValue(
        //                            //    $"@IdTercero{batchStartIndex + i}",
        //                            //   (mercap.IdTercero.HasValue && terceros.TryGetValue(mercap.IdTercero.Value, out var idT))
        //                            //       ? (object)idT
        //                            //       : DBNull.Value
        //                            //);


        //                            cmd.Parameters.AddWithValue(
        //                                $"@cajasmovimientosid{batchStartIndex + i}",
        //                                (mercap.numero != 0 &&
        //                                 cajasMovimientos.TryGetValue(mercap.numero.ToString(), out var idCaja))
        //                                    ? (object)idCaja
        //                                    : DBNull.Value
        //                            );


        //                            //cmd.Parameters.AddWithValue($"@fechayhorasolicitud{batchStartIndex + i}", dataf.fechayhorasolicitud);
        //                            //cmd.Parameters.AddWithValue($"@solicitudoperacion{batchStartIndex + i}", dataf.solicitudoperacion);
        //                            //cmd.Parameters.AddWithValue($"@solicitudvalor{batchStartIndex + i}", dataf.solicitudvalor);
        //                            //cmd.Parameters.AddWithValue($"@solicitudmonto{batchStartIndex + i}", dataf.solicitudmonto);
        //                            //cmd.Parameters.AddWithValue($"@solicitudiva{batchStartIndex + i}", dataf.solicitudiva);
        //                            //cmd.Parameters.AddWithValue($"@solicitudcodigocajero{batchStartIndex + i}", dataf.solicitudcodigocajero);
        //                            //cmd.Parameters.AddWithValue($"@respuestarespuesta{batchStartIndex + i}", dataf.respuestarespuesta);
        //                            //cmd.Parameters.AddWithValue($"@respuestaautorizacion{batchStartIndex + i}", dataf.respuestaautorizacion);
        //                            //cmd.Parameters.AddWithValue($"@respuestatarjeta{batchStartIndex + i}", dataf.respuestatarjeta);
        //                            //cmd.Parameters.AddWithValue($"@respuestatipotarjeta{batchStartIndex + i}", dataf.respuestatipotarjeta);
        //                            //cmd.Parameters.AddWithValue($"@respuestafranquicia{batchStartIndex + i}", dataf.respuestafranquicia);
        //                            //cmd.Parameters.AddWithValue($"@respuestamonto{batchStartIndex + i}", dataf.respuestamonto);
        //                            //cmd.Parameters.AddWithValue($"@respuestaiva{batchStartIndex + i}", dataf.respuestaiva);
        //                            //cmd.Parameters.AddWithValue($"@respuestarecibo{batchStartIndex + i}", respuestaRecibo);
        //                            //cmd.Parameters.AddWithValue($"@respuestacuotas{batchStartIndex + i}", dataf.respuestacuotas);
        //                            //cmd.Parameters.AddWithValue($"@respuestarrn{batchStartIndex + i}", respuestaRrn);
        //                            //cmd.Parameters.AddWithValue($"@respuestaobservacion{batchStartIndex + i}", dataf.respuestaobservacion);

        //                        }

        //                        Console.WriteLine($"Ejecutando lote {(contador / batchSize) + 1}...");

        //                        await cmd.ExecuteNonQueryAsync();
        //                        await tx.CommitAsync();

        //                        //Console.WriteLine($"Lote completado. Registros procesados: {contador}/{fpago_list.Count}");
        //                    }
        //                    catch (Exception batchEx)
        //                    {
        //                        Console.WriteLine($"Error en el lote (registros {contador + 1}-{contador}): {batchEx.Message}");
        //                        await tx.RollbackAsync();

        //                        // Opcional: continuar con el siguiente lote o terminar
        //                        // throw; // Descomenta si quieres que termine en el primer error
        //                    }
        //                }
        //            }
        //        }

        //        cronometer.Stop();
        //        TimeSpan tiempoTranscurrido = cronometer.Elapsed;
        //        Console.WriteLine($"Proceso completado:");
        //        Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours:F2} horas");
        //        Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes:F2} minutos");
        //        Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds:F2} segundos");
        //    }
        //    catch (Exception ex)
        //    {
        //        // ✅ Manejo seguro de excepciones
        //        Console.WriteLine("=== ERROR GENERAL ===");
        //        Console.WriteLine($"Mensaje: {ex.Message}");

        //        if (ex.InnerException != null)
        //        {
        //            Console.WriteLine($"Excepción interna: {ex.InnerException.Message}");
        //        }

        //        Console.WriteLine($"Stack Trace: {ex.StackTrace}");

        //        // Log adicional para debugging
        //        Console.WriteLine($"Tipo de excepción: {ex.GetType().Name}");
        //    }
        //}

        public static double CalcularRentabilidad(
            double pvtaxx, double pcosto, int pmodo, double piva,
            double pconsumo, int picon_1111,
            int pr_tipoiup, double pr_vr_ibu, DateTime pr_fech_iu, int pr_gen_iup)
            {
                if (double.IsNaN(picon_1111)) // FoxPro hace TYPE, aquí usamos NaN como equivalente inválido
                    picon_1111 = 0;

                double x_porc = 0.00;

                if (pvtaxx == 0 || pcosto == 0)
                    return 0;

                double xFact_iup = 0.00;
                double x_vr_ibu = 0.00;

                // Determinar impuesto saludable
                switch (pr_tipoiup)
                {
                    case 1:
                        xFact_iup = FPorcIcup(pr_fech_iu);
                        break;

                    case 2:
                        // Bebidas: no se hace nada según lógica actual
                        break;
                }

                double x_iva = 0;

                switch (pmodo)
                {
                    case 1:
                        if (pcosto > 0)
                        {
                            x_iva = 1 + (piva / 100.0);
                            x_porc = Math.Round(((pvtaxx / ((pcosto * x_iva) + pconsumo)) - 1) * 100, 2);
                        }
                        break;

                    case 2:
                        x_iva = 1 + (piva / 100.0);
                        x_porc = Math.Round((1 - ((pcosto + pconsumo) / (pvtaxx / x_iva))) * 100, 2);
                        break;

                    case 3:
                        x_iva = 1 + ((piva + xFact_iup) / 100.0);

                        if (picon_1111 == 1)
                        {
                            x_porc = pvtaxx / (((pcosto + pr_vr_ibu) * x_iva) + pconsumo);
                            x_porc = (x_porc - 1) * 100;
                            x_porc = Math.Round(x_porc, 2);
                        }
                        else
                        {
                            x_porc = Math.Round(((pvtaxx / ((pcosto + pr_vr_ibu) * x_iva)) - 1) * 100.0, 2);
                        }
                        break;

                    case 4:
                        x_iva = 1 + ((piva + xFact_iup) / 100.0);

                        if (picon_1111 == 1)
                        {
                            x_porc = pvtaxx / ((pcosto * x_iva) + pconsumo + pr_vr_ibu);
                            x_porc = (x_porc - 1) * 100;
                            x_porc = Math.Round(x_porc, 2);
                        }
                        else
                        {
                            x_porc = (pvtaxx - pconsumo) / ((pcosto * x_iva) + pr_vr_ibu);
                            x_porc = (x_porc - 1) * 100;
                            x_porc = Math.Round(x_porc, 2);
                        }
                        break;
                }

                if (x_porc > 999.99)
                    x_porc = 999.99;

                if (x_porc < 0)
                    x_porc = 0; // FoxPro no hace nada, puedes decidir si lo dejas en cero

                return x_porc;

            }


        private static double FPorcIcup(DateTime fecha)
        {
            // Simulación. Sustituir con la lógica real de cálculo del impuesto por fecha.
            int año = fecha.Year;
            if (año >= 2024) return 10.0;
            if (año == 2023) return 8.0;
            return 5.0;
        }


        public static decimal FCalcImp(
        decimal pVrBase,
        decimal pFiva,
        decimal pVrIconsu,
        int pModo,
        int pRound,
        int pmTipoIup,
        decimal pmVrIbu,
        DateTime pmFechIu,
        int pmGenIup,
        string pReturnUp)
        {
            int xNumRound = pRound;
            decimal xFactIup = 0.00m;
            decimal xVrIbu = 0.00m;

            if (pmGenIup == 1)
            {
                switch (pmTipoIup)
                {
                    case 1: // Comestibles
                        xFactIup = FporcIcup(pmFechIu);
                        break;
                    case 2: // Bebidas
                        xVrIbu = pmVrIbu;
                        break;
                }
            }

            decimal xVrBase = 0.00m;
            decimal xRetorno = 0.00m;
            decimal xBaseGrav = 0.00m;
            decimal xCalcIva = 0.00m;
            decimal xCalcUp = 0.00m;

            switch (pModo)
            {
                case 1: // Valor base con impuestos incluidos
                    xVrBase = pVrBase - pVrIconsu - xVrIbu;

                    decimal divisor = Math.Round(1 + Math.Round((pFiva + xFactIup) / 100.00m, 4), 4);
                    xBaseGrav = Math.Round(xVrBase / divisor, xNumRound);

                    if (pFiva > 0 && xFactIup == 0)
                    {
                        xCalcIva = xVrBase - xBaseGrav;
                        xCalcUp = pmTipoIup == 2 ? xVrIbu : 0;
                    }
                    else if (pFiva == 0 && xFactIup > 0)
                    {
                        xCalcIva = 0;
                        xCalcUp = xVrBase - xBaseGrav;
                    }
                    else if (pFiva > 0 && xFactIup > 0)
                    {
                        xCalcUp = Math.Round(xBaseGrav * Math.Round(xFactIup / 100.00m, 4), 2);
                        xCalcIva = xVrBase - xBaseGrav - xCalcUp;
                    }
                    else
                    {
                        xCalcIva = 0;
                        xCalcUp = pmTipoIup == 2 ? xVrIbu : 0;
                    }
                    break;

                case 2: // Valor base sin IVA ni ultraprocesados, pero con impoconsumo
                    xVrBase = pVrBase - pVrIconsu;

                    if (pFiva > 0 && xFactIup == 0)
                    {
                        xCalcIva = Math.Round(xVrBase * Math.Round(pFiva / 100.00m, 4), xNumRound);
                        xCalcUp = pmTipoIup == 2 ? xVrIbu : 0;
                    }
                    else if (pFiva == 0 && xFactIup > 0)
                    {
                        xCalcIva = 0;
                        xCalcUp = Math.Round(xVrBase * Math.Round(xFactIup / 100.00m, 4), xNumRound);
                    }
                    else if (pFiva > 0 && xFactIup > 0)
                    {
                        xCalcUp = Math.Round(xVrBase * Math.Round(xFactIup / 100.00m, 4), 2);
                        xCalcIva = Math.Round(xVrBase * Math.Round(pFiva / 100.00m, 4), 2);
                    }
                    else
                    {
                        xCalcIva = 0;
                        xCalcUp = pmTipoIup == 2 ? xVrIbu : 0;
                    }
                    break;
            }

            switch (pReturnUp)
            {
                case "U":
                    xRetorno = xCalcUp;
                    break;
                case "I":
                    xRetorno = xCalcIva;
                    break;
                case "A":
                    xRetorno = xCalcIva + xCalcUp;
                    break;
            }

            return xRetorno;
        }


        public static decimal FporcIcup(DateTime pFechCaus)
        {
            decimal xIcup = 0.00m;

            if (pFechCaus.Year == 2023)
            {
                DateTime xFeIcup = new DateTime(2023, 11, 1);

                if (pFechCaus >= xFeIcup)
                {
                    xIcup = 10.00m;
                }
            }
            else if (pFechCaus.Year == 2024)
            {
                xIcup = 15.00m;
            }
            else if (pFechCaus.Year == 2025)
            {
                xIcup = 20.00m;
            }

            return xIcup;
        }

        public static void InsertarDocumentos(List<t_docum> docum_list)
        {


            var cronometer = Stopwatch.StartNew();
            Stopwatch sw = new Stopwatch();
            sw.Start();


            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();
                //Iniciar transaccion
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    foreach (var docum in docum_list)
                    {
                        try
                        {
                            cmd.Parameters.Clear();

                            // Comando SQL con parámetros
                            cmd.CommandText = $@"INSERT INTO public.""Documentos""(""Codigo"", ""Nombre"", ""Asimilar"", ""CD"", ""CT"",
                                ""IN"", ""VD"", ""UV"", ""CR"", ""PC"", ""DO"", ""TR"", ""CA"", ""DD"", ""LT"", ""NL"", ""RD"", ""RO"",
                                ""ControlFechas"", ""Vendedor"", ""Zona"", ""CCosto"", ""Resolucion"", ""ActivarColumna"", ""ControlaPagos"",
                                ""Cuentas"", ""FechaCreacion"", ""IdDocumentoContrapartida"", ""Naturaleza"", ""Detalles"", ""Mensaje1"",
                                ""Mensaje2"", ""Mensaje3"", ""ValoresCartera"", ""Anexo1"", ""Anexo2"", ""Anexo3"", ""Anexo4"", ""Anexo5"",
                                ""Anexo6"", ""MovimientoCartera"", ""FusionarDocumento"")VALUES (@Codigo,@Nombre,@Asimilar,@CD,@CT,
                                @IN,@VD,@UV,@CR,@PC,@DO,@TR,@CA,@DD,@LT,@NL,@RD,@RO,@ControlFechas,@Vendedor,@Zona,@CCosto,@Resolucion,
                                @ActivarColumna,@ControlaPagos,@Cuentas,@FechaCreacion,@IdDocumentoContrapartida,@Naturaleza,@Detalles,@Mensaje1,
                                @Mensaje2,@Mensaje3,@ValoresCartera,@Anexo1,@Anexo2,@Anexo3,@Anexo4,@Anexo5,@Anexo6,@MovimientoCartera,@FusionarDocumento                                            
                                )";

                            long bTipodoc = docum.tipo_doc; //se convierte a bigint. long es lo mismo que bigint


                            cmd.Parameters.AddWithValue("@Codigo", docum.docum);
                            cmd.Parameters.AddWithValue("@Nombre", docum.nombre);
                            cmd.Parameters.AddWithValue("@Asimilar", bTipodoc);
                            cmd.Parameters.AddWithValue("@CD", docum.contabil == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@CT", docum.si_cnomb == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@IN", docum.bloqueado == 1 ? false : true);
                            cmd.Parameters.AddWithValue("@VD", docum.vali_doc == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@UV", docum.si_consec == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@CR", docum.controlrut == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@PC", docum.camb_ter == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@DO", docum.desc_ord == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@TR", docum.es_trans == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@CA", docum.cons_proc == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@DD", docum.desc_doci == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@LT", docum.silibtes == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@NL", docum.n_lineas > 1 ? true : false);
                            cmd.Parameters.AddWithValue("@RD", docum.n_recup == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@RO", docum.obser_doc == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@ControlFechas", docum.cont_fec);
                            cmd.Parameters.AddWithValue("@Vendedor", docum.vend_det == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@Zona", docum.zon_det == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@CCosto", docum.cco_det == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@Resolucion", docum.es_resolu == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@ActivarColumna", docum.sniif_on == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@ControlaPagos", docum.si_contpag == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@Cuentas", false);
                            cmd.Parameters.AddWithValue("@FechaCreacion", docum.fecha_cre);
                            cmd.Parameters.AddWithValue("@IdDocumentoContrapartida", DBNull.Value);
                            cmd.Parameters.AddWithValue("@Naturaleza", 1);
                            cmd.Parameters.AddWithValue("@Detalles", "");
                            cmd.Parameters.AddWithValue("@Mensaje1", docum.Mensaje1);
                            cmd.Parameters.AddWithValue("@Mensaje2", docum.Mensaje2);
                            cmd.Parameters.AddWithValue("@Mensaje3", docum.Mensaje3);
                            cmd.Parameters.AddWithValue("@ValoresCartera", docum.afin_cxc);
                            cmd.Parameters.AddWithValue("@Anexo1", docum.Anexo1);
                            cmd.Parameters.AddWithValue("@Anexo2", docum.Anexo2);
                            cmd.Parameters.AddWithValue("@Anexo3", docum.Anexo3);
                            cmd.Parameters.AddWithValue("@Anexo4", docum.Anexo4);
                            cmd.Parameters.AddWithValue("@Anexo5", docum.Anexo5);
                            cmd.Parameters.AddWithValue("@Anexo6", docum.Anexo6);
                            cmd.Parameters.AddWithValue("@MovimientoCartera", docum.afin_tipo);
                            cmd.Parameters.AddWithValue("@FusionarDocumento", docum.afin_doc);

                            // Ejecutar el comando de inserción
                            cmd.ExecuteNonQuery();

                            contador++; // Incrementar contador
                            Console.WriteLine($"Documento insertado #{contador}"); // Mostrar en consola
                        }

                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar cliente con documento: {ex.InnerException}");
                        }
                    }
                    // Confirmar la transacción
                    tx.Commit();
                }
                conn.Close();
            }
            Console.WriteLine("Datos insertados correctamente.");
            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
            Console.ReadKey();
        }


        public static void InsertarIncapacidadesNomina(List<t_inc_desc> inc_desc_list)
        {
            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();
                //Iniciar transaccion
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    foreach (var inc_desc in inc_desc_list)
                    {
                        try
                        {
                            cmd.Parameters.Clear(); // ← CRUCIA

                            // Comando SQL con parámetros
                            cmd.CommandText = $@"INSERT INTO public.""IncapacidadesNomina""(periodo, tipo, descuentodiasarp, entidadid, empleadoid,
                                    fechainicio, fechafinal, valorincapacidadsalud, valorincapacidadriesgos, valorlicenciamaternidad,
                                    valorincapacidadsaludautorizacion, valorincapacidadriesgosautorizacion, valorlicenciamaternidadautorizacion,
                                    aportespagadosaotrossubsistemas) VALUES (@periodo,@tipo,@descuentodiasarp,
                                    (SELECT DISTINCT id FROM ""EntidadesNomina"" WHERE codigo = @entidadid LIMIT 1),
                                    (SELECT DISTINCT ""Id"" FROM ""Empleados"" WHERE ""CodigoEmpleado"" = @empleadoid LIMIT 1),
                                    @fechainicio,@fechafinal,@valorincapacidadsalud,@valorincapacidadriesgos,@valorlicenciamaternidad,
                                    @valorincapacidadsaludautorizacion,@valorincapacidadriesgosautorizacion,@valorlicenciamaternidadautorizacion,
                                    @aportespagadosaotrossubsistemas)";


                            cmd.Parameters.AddWithValue("@periodo", inc_desc.periodo);
                            cmd.Parameters.AddWithValue("@tipo", inc_desc.entidad);
                            cmd.Parameters.AddWithValue("@descuentodiasarp", inc_desc.solo_arp == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@entidadid", inc_desc.cod_ent);
                            cmd.Parameters.AddWithValue("@empleadoid", inc_desc.empleado);
                            cmd.Parameters.AddWithValue("@fechainicio", inc_desc.fecha_ini);
                            cmd.Parameters.AddWithValue("@fechafinal", inc_desc.fecha_fin);
                            cmd.Parameters.AddWithValue("@valorincapacidadsalud", inc_desc.vr_inc_sal);
                            cmd.Parameters.AddWithValue("@valorincapacidadriesgos", inc_desc.vr_inc_arp);
                            cmd.Parameters.AddWithValue("@valorlicenciamaternidad", inc_desc.vr_inc_mat);
                            cmd.Parameters.AddWithValue("@valorincapacidadsaludautorizacion", inc_desc.au_inc_sal != "" ? int.Parse(inc_desc.au_inc_sal) : DBNull.Value);
                            cmd.Parameters.AddWithValue("@valorincapacidadriesgosautorizacion", inc_desc.au_inc_arp != "" ? int.Parse(inc_desc.au_inc_arp) : DBNull.Value);
                            cmd.Parameters.AddWithValue("@valorlicenciamaternidadautorizacion", inc_desc.au_inc_mat != "" ? int.Parse(inc_desc.au_inc_mat) : DBNull.Value);
                            cmd.Parameters.AddWithValue("@aportespagadosaotrossubsistemas", inc_desc.apor_pag);


                            // Ejecutar el comando de inserción
                            cmd.ExecuteNonQuery();

                            contador++; // Incrementar contador
                            Console.WriteLine($"Documento insertado #{contador}"); // Mostrar en consola
                        }

                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar cliente con documento: {ex.InnerException}");
                            //conn.Close();
                        }
                    }
                    // Confirmar la transacción
                    tx.Commit();
                }
                conn.Close();
            }
            Console.WriteLine("Datos insertados correctamente.");
        }



        public static string GetInterpolatedQuery(NpgsqlCommand cmd)
        {
            string query = cmd.CommandText;

            foreach (NpgsqlParameter param in cmd.Parameters)
            {
                string paramName = param.ParameterName;
                object value = param.Value;

                string formattedValue;

                if (value == null || value == DBNull.Value)
                {
                    formattedValue = "NULL";
                }
                else if (value is string strVal)
                {
                    formattedValue = $"'{strVal.Replace("'", "''")}'";
                }
                else if (value is DateTime dtVal)
                {
                    formattedValue = $"'{dtVal:yyyy-MM-dd HH:mm:ss}'";
                }
                else if (value is bool boolVal)
                {
                    formattedValue = boolVal ? "TRUE" : "FALSE";
                }
                else
                {
                    formattedValue = value.ToString();
                }

                // Reemplazar solo el nombre del parámetro exacto (evitar que @a reemplace dentro de @anexo, por ejemplo)
                query = Regex.Replace(query, $@"(?<!\w){Regex.Escape(paramName)}(?!\w)", formattedValue);
            }

            return query;
        }


    }       
}
