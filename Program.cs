using System;
using System.Collections;
using System.Data.OleDb;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Runtime.Intrinsics.X86;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using DotNetDBF;
using Migracion;
using Migracion.Clases;
using Migracion.Utilidades;
using Npgsql;
using static System.Runtime.InteropServices.JavaScript.JSType;

class Program
{
    // 12-05-2025 Generado por : CarlosM
    static async Task Main(string[] args)
    
    
    {

 
        //Instanciando la clase proceso
        Procesos pro = new Procesos();
        Utilidades utilidades = new Utilidades();

        //Provider
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

        int xTabla = 0;
        int xInicio = 0;
        int xFin = 0;

        // Solicitando la clase a procesar
        Console.WriteLine("Seleccione el tipo de proceso One to one o how many:");
        int xProcess = int.Parse(Console.ReadLine());


        if (xProcess ==1)
        {

            Console.WriteLine("Por favor seleccionar tabla:");
            xTabla = int.Parse(Console.ReadLine());
            procedure(xTabla).GetAwaiter().GetResult(); ;

        }
        else
        {
            //Console.WriteLine("Ingresar el numero de Rango, pero que sean continuos :)");
            //Console.WriteLine("Valor de inicio: ");
            //xInicio = int.Parse(Console.ReadLine());
            //Console.WriteLine("Valor Final: ");
            //xFin = int.Parse(Console.ReadLine());

            //for (int i = xInicio; i <= xFin; i++)
            //{
            //   procedure(i).GetAwaiter().GetResult();

            //}

            Console.WriteLine("Ingresar el numero de Rango, pero que sean continuos :)");
            string input = Console.ReadLine();

            List<int> numeros = input
                .Split(',')
                .Select(n => int.Parse(n.Trim()))
                .ToList();

            foreach (int n in numeros)
            {
                await procedure(n);
            }



        }
        
        
    }

    public static async Task procedure(int xProce)
    {

        //notas antes de correr fenalpag se debe de correr fenalpagconfiguracion ya que esta tabla no tiene alguna afectacion o necesite alguna tabla hija hablando de fenalpagconfiguracion

        //Variables que contiene las rutas de las tablas de FoxPro
        string dbfFilePath = @"C:\tmp_archivos\vended.dbf";
        string dbfFileCliente = @"C:\tmp_archivos\anexosFin.dbf";
        string dbfFileCiudad = @"C:\tmp_archivos\ciudad.DBF";
        string dbfFileHistoNove = @"C:\tmp_archivos\histonove.DBF";
        string dbfFileHistonve_P = @"C:\tmp_archivos\histonove_p.dbf";
        string dbfFileHistonove_S = @"c:\tmp_archivos\histonove_s.dbf";
        string dbfFileDocum = @"c:\tmp_archivos\docum.dbf";
        //string dbfFileitems       = @"c:\tmp_archivos\items.dbf";
        //string dbfFileitems = @"c:\tmp_archivos\itemsA.dbf";
        //string dbfFileitems = @"c:\tmp_archivos\items_f.dbf";
        string dbfFileitems = @"c:\tmp_archivos\items.dbf";
        string dbfFenalpag = @"c:\tmp_archivos\fenalpag.dbf";
        string dbfMovnom = @"c:\tmp_archivos\movnomact.dbf";
        string dbfPlanDet = @"C:\tmp_archivos\plan_det.dbf";
        string dbfDcom = @"C:\tmp_archivos\dcom.DBF";
        string dbfFCreditos = @"C:\tmp_archivos\creditos.DBF";
        string dbfCabFact = @"C:\tmp_archivos\movd.DBF";
        string dbfDetFact = @"C:\tmp_archivos\movp.DBF";
        //string dbfDetFact = @"C:\tmp_archivos\movpp.DBF";
        
        string dbfFileinc_desc = @"c:\tmp_archivos\migracion\inc_desc.dbf";//copia de J:
        string dbfTercero = @"C:\tmp_archivos\anexosFin.dbf";
        //string dbfTercero = @"C:\tmp_archivos\faltantes.dbf"; 
        string dbfFpagos = @"C:\tmp_archivos\movp_nv.DBF";
        string dbfdataf = @"C:\tmp_archivos\dataf25.DBF";
        string dbfSeparados = @"C:\tmp_archivos\separados.DBF";
        string dbfVentas = @"C:\tmp_archivos\ventas.DBF";
        string dbfCuotas = @"C:\tmp_archivos\ABONOSFIN.DBF";
        string dbfcred2 = @"C:\tmp_archivos\detcre2.DBF";
        string dbfcredFin = @"C:\tmp_archivos\creditos.DBF";
        //string dbfdetras = @"C:\tmp_archivos\detras.DBF";
        string dbfdetras = @"C:\tmp_archivos\movdesp.DBF";
        string dbfFileBonodev = @"C:\tmp_archivos\bonodevc.dbf";
        string dbfMpmovi      = @"C:\tmp_archivos\mpmovi25.dbf";
        string dbfMercapesos  = @"C:\tmp_archivos\TCCLIEN5_LIMPIO.dbf";
        string dbfDetalleMovite      = @"C:\tmp_archivos\movite25a.dbf";
        string dbfMovite = @"C:\tmp_archivos\cab_movite.dbf"; 
        //string dbfMovite       = @"C:\tmp_archivos\pruebamov.dbf"; 
        string codigoscuen       = @"J:\sistemas\Migracion\codigoscuen.dbf ";
        string cabdoc            = @"C:\tmp_archivos\cabdoc25.dbf";
        string dbfmovdoc         = @"C:\tmp_archivos\docisto.dbf";
        string dbfdetalleMovdoc  = @"C:\tmp_archivos\movdoc.dbf";
        string dbfInventarios    = @"C:\tmp_archivos\movite25a.dbf";
        string dbfPPYE           = @"C:\tmp_archivos\modcPPYE.dbf";
        string dbfMovdesp        = @"J:\sistemas\migracion\movdesp.dbf";
        string dbfModel_contable = @"C:\tmp_archivos\modc_ppe.dbf";
        string dbfFacturas       = @"C:\tmp_archivos\tempo_final.dbf";
        string dbfDetLiqFact = @"C:\tmp_archivos\det_liqf.dbf";
        string dbfConLiqFact = @"C:\tmp_archivos\con_liqf.dbf";
        string dbfConfig5 = @"C:\tmp_archivos\config5ant.dbf";
        string dbfConfig = @"C:\tmp_archivos\config.dbf";
        string dbfcabcaptu = @"C:\tmp_archivos\cabcaptu.dbf";
        string dbflocaliza = @"C:\tmp_archivos\localiza.dbf";
        string dbfubicacion = @"C:\tmp_archivos\ubicacion.dbf";
        string dbfdetcaptu = @"C:\tmp_archivos\detcaptu.dbf";
        string dbfusucaptu = @"C:\tmp_archivos\usucaptu.dbf";
        string dbfisiscaptu = @"C:\tmp_archivos\capturas.dbf";
        string dbfFileTelibro = @"C:\tmp_archivos\telibro.dbf"; // 10/12/2025
        string dbfSisliqcre = @"C:\tmp_archivos\sisliqcre.dbf";

        //string dbfmovdoc = @"C:\tmp_archivos\movdoc.dbf";




        //DETCRE2 

        // Son listas que se generar para almacenar todos los datos de las
        // tablas del FoxPro
        var vendedList = new List<t_vended>();
        var Clien_list = new List<t_cliente>();
        var ciudade_lis = new List<t_ciudad>();
        var nove_list = new List<t_histonove>();
        var nove_pen = new List<t_histonove>();
        var nove_sal = new List<t_histonove>();
        var items_list = new List<t_items>();
        var docum_list = new List<t_docum>();
        var fenal_list = new List<t_fenalpag>();
        var movnomList = new List<t_movnom>();
        var PlanDetList = new List<t_planDet>();
        var DcomList = new List<t_dcom>();
        var creditosList = new List<t_fenalCredtos>();
        var cabfactList = new List<t_cabFact>();
        var detfactList = new List<t_detFact>();
        var inc_desc_list = new List<t_inc_desc>();
        var tercero_list = new List<t_terceros>();
        var fpago_list = new List<t_fpago>();
        var dataf_list = new List<t_dataf>();
        var separados_list = new List<t_separados>();
        var ventas_list = new List<t_ventas>();
        var cuotas_list = new List<t_cuotas>();
        var cred_list = new List<t_cred2>();
        var cCuota_list = new List<t_credCuotas>();
        var detras_list = new List<t_despacho>();
        var bonodev_lst = new List<t_bonodev>();
        var mpmovi_list = new List<t_mpmovi>();
        var merca_list  = new List<t_mercapesos>();
        var movite_list = new List<t_mov_inventarios>();
        var Detmovite_list = new List<t_det_movInventario>();
        var cuen_list   = new List<t_cuen>();
        var cabdoc_list = new List<t_cabdoc>();
        var movdoc_list = new List<t_mov_contables>();
        var detMovdoc_list = new List<t_det_movContable>();
        var inventarios_list = new List<t_inventario>();
        var movdesp_list     = new List<t_movdesp>();
        var modelConta_list  = new List<t_modelContable>();
        var facturaPro_list = new List<t_facturasPro>();
        var detLiqf_list = new List<t_detliqf>();
        var conLiqf_list = new List<t_conliqf>();
        var config5_list = new List<t_config5>();
        var config_list = new List<t_config>();
        var cabcaptu_list = new List<t_cabcaptu>();
        var localiza_list = new List<t_localiza>();
        var ubicacion_list = new List<t_ubicacion>();
        var detcaptu_list = new List<t_detcaptu>();
        var usucaptu_list = new List<t_usucaptu>();
        var isiscaptu_list = new List<t_isiscaptu>();
        var telibro_list = new List<t_telibro>();
        var sisliqcre_list = new List<t_sisliqcre>();


        switch (xProce)
        {
            case 1:  // using de vended

                using (FileStream fs = File.OpenRead(dbfFilePath))
                {
                    var reader = new DBFReader(fs);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var vendedor = new t_vended
                        {
                            Vended = record[0]?.ToString().Trim(),
                            Nombre = record[1]?.ToString().Trim(),
                            Cedula = Convert.ToInt32(record[2]),
                            Tel = record[3]?.ToString().Trim(),
                            Direcc = record[4]?.ToString().Trim(),
                            Ciudad = record[5]?.ToString().Trim(),
                            FechIng = Convert.ToDateTime(record[6]),
                            Ccosto = record[7]?.ToString().Trim()
                        };

                        vendedList.Add(vendedor);
                    }
                }

                Procesos.InsertarVendedores(vendedList);

                break;
            case 2:  //using clientes y el de ciudad

                using (FileStream fs = File.OpenRead(dbfFileCliente))
                {
                    var reader = new DBFReader(fs);
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252); // ANSI típico de VFP

                    object[] record;
                    int fila = 0;
                    while (true)
                    {
                        try
                        {
                            record = reader.NextRecord();
                            if (record == null) break;

                            fila++;

                            string raw82 = record[82]?.ToString()?.Trim();
                            string tdoc = string.IsNullOrWhiteSpace(raw82) ? "13" : raw82;

                            var cliente = new t_cliente
                            {

                                tdoc = tdoc,
                                anexo = record[1]?.ToString()?.Trim(),
                                nombre = record[2]?.ToString()?.Trim(),
                                dv = record[4]?.ToString()?.Trim(),
                                direcc = record[5]?.ToString()?.Trim(),
                                emailfe1 = record[144]?.ToString()?.Trim(),
                                tel = record[9]?.ToString()?.Trim(),
                                apl1 = record[83]?.ToString()?.Trim(),
                                apl2 = record[84]?.ToString()?.Trim(),
                                nom1 = record[85]?.ToString()?.Trim(),
                                nom2 = record[86]?.ToString()?.Trim(),
                                Dane = record[74]?.ToString()?.Trim(),
                                tipo_per = record[109]?.ToString()?.Trim(),
                                bloqueado = record[12] != null && record[12].ToString() == "1" ? 1 : 0
                            };



                            Clien_list.Add(cliente);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error en fila {fila}: {ex.Message}");
                            continue; // o break si no quieres continuar
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfFileCiudad))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var ciudad = new t_ciudad
                        {
                            Municipio = record[1]?.ToString()?.Trim(),
                            Departamento = record[2]?.ToString()?.Trim(),
                            Dane = record[3]?.ToString()?.Trim()
                        };

                        ciudade_lis.Add(ciudad);

                    }
                }

                await Procesos.InsertarClientes(Clien_list, ciudade_lis);

                break;
            case 3: // cambioEps

                using (FileStream fc = File.OpenRead(dbfFileHistoNove))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var histonove = new t_histonove
                        {
                            cod_ant = record[4]?.ToString()?.Trim(),
                            cod_act = record[5]?.ToString()?.Trim(),
                            usua_reg = record[9]?.ToString()?.Trim(),
                            fech_reg = Convert.ToDateTime(record[7]),
                            hora_reg = TimeOnly.Parse(record[8].ToString()),
                            empleado = record[0]?.ToString()?.Trim(),
                            fech_camb = Convert.ToDateTime(record[6]),
                        };

                        nove_list.Add(histonove);

                    }
                }

                Procesos.InsertarHistoNove(nove_list);

                break;
            case 4:  // cambioPension

                using (FileStream fc = File.OpenRead(dbfFileHistonve_P))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var histonoveP = new t_histonove
                        {
                            cod_ant = record[4]?.ToString()?.Trim(),
                            cod_act = record[5]?.ToString()?.Trim(),
                            usua_reg = record[9]?.ToString()?.Trim(),
                            fech_reg = Convert.ToDateTime(record[7]),
                            hora_reg = TimeOnly.Parse(record[8].ToString()),
                            empleado = record[0]?.ToString()?.Trim(),
                            fech_camb = Convert.ToDateTime(record[6]),
                        };

                        nove_list.Add(histonoveP);

                    }
                }

                Procesos.InsertarHistonoveP(nove_list);


                break;
            case 5:

                using (FileStream fc = File.OpenRead(dbfFileHistonove_S))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var histonoveP = new t_histonove
                        {
                            val_ant = Convert.ToInt32(record[2]),
                            val_act = Convert.ToInt32(record[3]),
                            usua_reg = record[9]?.ToString()?.Trim(),
                            fech_reg = Convert.ToDateTime(record[7]),
                            hora_reg = TimeOnly.Parse(record[8].ToString()),
                            empleado = record[0]?.ToString()?.Trim(),
                            fech_camb = Convert.ToDateTime(record[6]),
                        };

                        nove_sal.Add(histonoveP);

                    }
                }

                Procesos.InsertarHistonoveS(nove_sal);

                break;
            case 6://documentos

                using (FileStream fc = File.OpenRead(dbfFileDocum))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var documentos = new t_docum
                        {
                            docum = record[0]?.ToString()?.Trim(),   //codigo
                            nombre = record[1]?.ToString()?.Trim(),  //nombre
                            tipo_doc = Convert.ToInt32(record[4]),  //asimilar
                            contabil = Convert.ToInt32(record[18]),   //CD
                            si_cnomb = Convert.ToInt32(record[40]),   //CT
                            bloqueado = Convert.ToInt32(record[36]),  //IN
                            vali_doc = Convert.ToInt32(record[83]),   //VD
                            si_consec = Convert.ToInt32(record[105]),  //UV
                            controlrut = Convert.ToInt32(record[120]), //CR
                            camb_ter = Convert.ToInt32(record[137]),   //PC
                            desc_ord = Convert.ToInt32(record[138]),   //DO
                            es_trans = Convert.ToInt32(record[143]),   //TR
                            cons_proc = Convert.ToInt32(record[144]),  //CA
                            desc_doci = Convert.ToInt32(record[145]),  //DD
                            silibtes = Convert.ToInt32(record[147]),   //LT
                            n_lineas = Convert.ToInt32(record[2]),   //NL, esta campo para nosotros es numerico, ellos lo tienen boleano
                            n_recup = Convert.ToInt32(record[156]),    //RD
                            obser_doc = Convert.ToInt32(record[178]),  //RO
                            cont_fec = Convert.ToInt32(record[48]),   //ControlFechas
                            vend_det = Convert.ToInt32(record[28]),   //Vendedor
                            zon_det = Convert.ToInt32(record[29]),    //Zona
                            cco_det = Convert.ToInt32(record[30]),    //CCosto
                            es_resolu = Convert.ToInt32(record[149]),  //Resolucion
                            sniif_on = Convert.ToInt32(record[166]),   //ActivarColumna
                            si_contpag = Convert.ToInt32(record[167]), //ControlaPagos
                            fecha_cre = Convert.ToDateTime(record[19]),//FechaCreacion
                            Mensaje1 = record[31]?.ToString()?.Trim(),//Mensaje1
                            Mensaje2 = record[32]?.ToString()?.Trim(),//Mensaje2
                            Mensaje3 = record[33]?.ToString()?.Trim(),//Mensaje3
                            afin_cxc = Convert.ToInt32(record[129]),   //ValoresCartera
                            Anexo1 = record[22]?.ToString()?.Trim(),  //Anexo1
                            Anexo2 = record[23]?.ToString()?.Trim(),  //Anexo2
                            Anexo3 = record[24]?.ToString()?.Trim(),  //Anexo3
                            Anexo4 = record[25]?.ToString()?.Trim(),  //Anexo4
                            Anexo5 = record[26]?.ToString()?.Trim(),  //Anexo5
                            Anexo6 = record[27]?.ToString()?.Trim(),  //Anexo6
                            afin_tipo = Convert.ToInt32(record[132]),  //MovimientoCartera
                            afin_doc = record[131]?.ToString()?.Trim(),//FusionarDocumento
                        };
                        docum_list.Add(documentos);
                    }
                }
                Procesos.InsertarDocumentos(docum_list);


                break;


            case 7: // items

                using (FileStream fc = File.OpenRead(dbfFileitems))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var itemsPick = new t_items
                        {
                            Codigo = record[0]?.ToString()?.Trim(),
                            tipo = record[12]?.ToString()?.Trim(),
                            fecha_cre = Convert.ToDateTime(record[11]),
                            nombre = record[1]?.ToString()?.Trim(),
                            shortname = record[181]?.ToString()?.Trim(),
                            refabrica = record[86]?.ToString()?.Trim(),
                            peso_uni = Convert.ToInt32(record[61]),
                            undxcaja = Convert.ToInt32(record[57]),
                            subgrupo = record[58]?.ToString()?.Trim(),
                            marca = record[42]?.ToString()?.Trim(),
                            pdrenado = Convert.ToInt32(record[160]),
                            cod_ean8 = record[42]?.ToString()?.Trim(),
                            cod_bar = record[43]?.ToString()?.Trim(),
                            bloqueado = Convert.ToInt32(record[18]),
                            unidad = record[19]?.ToString()?.Trim(),
                            talla = record[87]?.ToString()?.Trim(),
                            linea = record[4]?.ToString()?.Trim(),
                            sublinea = record[5]?.ToString()?.Trim(),
                            no_compra = Convert.ToInt32(record[121]),
                            es_kitpro = Convert.ToInt32(record[138]),
                            iva = Convert.ToInt32(record[6]),
                            pvta1i = Convert.ToInt32(record[7]),
                            pvta_a1 = Convert.ToInt32(record[20]),
                            cambiopv_1 = Convert.ToInt32(record[92]),
                            iconsumo = Convert.ToInt32(record[49]),
                            excluido = Convert.ToInt32(record[44]),
                            listap = Convert.ToInt32(record[79]),
                            F_ICONSUMO = Convert.ToInt32(record[135]),
                            costoajus = Convert.ToInt32(record[127]),
                            es_fruver = Convert.ToInt32(record[83]),
                            bolsa = Convert.ToInt32(record[104]),
                            mod_ppos = Convert.ToInt32(record[47]),
                            fenalce = record[103]?.ToString()?.Trim(),
                            acu_tpos = Convert.ToInt32(record[48]),
                            subsidio = Convert.ToInt32(record[105]),
                            mod_qpos = Convert.ToInt32(record[84]),
                            es_bol = record[132]?.ToString()?.Trim(),
                            contabgrav = Convert.ToInt32(record[96]),
                            sitoledo = Convert.ToInt32(record[88]),
                            pref_ean = record[89]?.ToString()?.Trim(),
                            es_bordado = Convert.ToInt32(record[100]),
                            es_moto = Convert.ToInt32(record[122]),
                            sipedido = Convert.ToInt32(record[136]),
                            escodensa = record[133]?.ToString()?.Trim(),
                            es_ingreso = Convert.ToInt32(record[129]),
                            cheqpr = Convert.ToInt32(record[131]),
                            si_descto = Convert.ToInt32(record[36]),
                            descmax = Convert.ToInt32(record[50]),
                            deci_cant = Convert.ToInt32(record[85]),
                            fech_cp1 = Convert.ToDateTime(record[9]),
                            costo_rep = Convert.ToInt32(record[14]),
                            cod_alt = record[3]?.ToString()?.Trim(),
                            CCosto = record[33]?.ToString()?.Trim(),
                            elegido = Convert.ToInt32(record[55]),
                            sdo_rojo = Convert.ToInt32(record[60]),
                            pidempeso = Convert.ToInt32(record[140]),
                            corrosivo = Convert.ToInt32(record[143]),
                            n_cas = record[141]?.ToString()?.Trim(),
                            cat_cas = record[142]?.ToString()?.Trim(),
                            vta_costo = Convert.ToInt32(record[62]),
                            si_detdoc = Convert.ToInt32(record[69]),
                            solocant = Convert.ToInt32(record[91]),
                            si_serie = Convert.ToInt32(record[125]),
                            terceAutom = Convert.ToInt32(record[126]),
                            generico = Convert.ToInt32(record[161]),
                            pesocajb = Convert.ToInt32(record[63]),
                            pesocajn = Convert.ToInt32(record[64]),
                            peso_car = Convert.ToInt32(record[159]),
                            unidmin = Convert.ToInt32(record[65]),
                            puntaje = Convert.ToInt32(record[112]),
                            fecha1_mp = Convert.ToDateTime(record[113]),
                            fecha2_mp = Convert.ToDateTime(record[114]),
                            desc_esp = Convert.ToInt32(record[107]),
                            fact_esp = Convert.ToInt32(record[108]),
                            valor_esp = Convert.ToInt32(record[109]),
                            fechdesci = Convert.ToDateTime(record[162]),
                            fechdescf = Convert.ToDateTime(record[163]),
                            descod = Convert.ToInt32(record[110]),
                            descod_f = Convert.ToDateTime(record[111]),
                            fchdescusr = Convert.ToDateTime(record[130]),
                            validmax = Convert.ToInt32(record[145]),
                            maxventa = Convert.ToInt32(record[146]),
                            val_desp = Convert.ToInt32(record[148]),
                            f_bloqp = Convert.ToDateTime(record[156]),
                            cont_devol = Convert.ToInt32(record[116]),
                            pvta3i = Convert.ToInt32(record[52]),
                            no_invped = Convert.ToInt32(record[107]),
                            aut_trasl = Convert.ToInt32(record[115]),
                            lp_cyvig = Convert.ToInt32(record[118]),
                            chequeo = Convert.ToInt32(record[106]),
                            costeo2 = Convert.ToInt32(record[119]),
                            sobrestock = Convert.ToInt32(record[123]),
                            Asopanela = Convert.ToInt32(record[124]),
                            grupodes = record[82]?.ToString()?.Trim(),
                            ean_1 = record[80]?.ToString()?.Trim(),
                            ean_2 = record[81]?.ToString()?.Trim(),
                            inandout = Convert.ToInt32(record[155]),
                            domi_com = Convert.ToInt32(record[107]),
                            ord_prio = Convert.ToInt32(record[164]),
                            modq_reg = Convert.ToInt32(record[178]),
                            mod_toler = Convert.ToInt32(record[179]),
                            stockdomi = Convert.ToInt32(record[169]),
                            ext_covid = Convert.ToInt32(record[173]),
                            unidcantfe = record[170]?.ToString()?.Trim(),
                            tipobiend = record[176]?.ToString()?.Trim(),
                            pdiasiva = Convert.ToInt32(record[177]),
                            es_dfnfp = Convert.ToInt32(record[149]),
                            varifnfp = Convert.ToInt32(record[150]),
                            DESFINNIIF = Convert.ToInt32(record[147]),
                            bod_asoc = record[39]?.ToString()?.Trim(),
                            pvtali = Convert.ToInt32(record[7]),
                            descuento = Convert.ToInt32(record[139]),
                            por_rentab = Convert.ToInt32(record[193]),
                            confirpre = Convert.ToInt32(record[137]),
                            ofertado = Convert.ToInt32(record[171]),
                            fech1comp = Convert.ToDateTime(record[144]),
                            imp_salu = Convert.ToInt32(record[195]),
                            vr_imps = Convert.ToInt32(record[196]),
                            cod_ref = record[90]?.ToString()?.Trim(),
                            refer = record[2]?.ToString()?.Trim(),


                        };

                        items_list.Add(itemsPick);

                    }
                }

                Procesos.InsertarProductos(items_list);
                break;
            //en el aprendizaje se aprende. cpd
            /* case 8: //bono devolu catrelina

                 using (FileStream fs = File.OpenRead(dbfFileBonodev))
                 {
                     var reader = new DBFReader(fs);
                     reader.CharEncoding = System.Text.Encoding.GetEncoding(1252); // ANSI típico de VFP

                     object[] record;
                     int fila = 0;

                     while (true)
                     {
                         try
                         {
                             record = reader.NextRecord();
                             if (record == null) break;

                             fila++;

                             var bonodevol = new t_bonodev
                             {

                                 fecha = Convert.ToDateTime(record[0]),
                                 super = record[1]?.ToString().Trim(),
                                 num_bono = Convert.ToInt32(record[2]),
                                 valorbono = Convert.ToInt32(record[3]),
                                 terceroLong = Convert.ToInt64(record[5]),
                                 suc_dev = record[6]?.ToString().Trim(),
                                 doc_dev = record[7]?.ToString().Trim(),
                                 cons_dev = record[8]?.ToString().Trim(),
                                 num_dev = Convert.ToInt32(record[9]),
                                 concepto = record[10]?.ToString().Trim(),
                                 fech_reg = Convert.ToDateTime(record[11]),
                                 hora_reg = TimeOnly.Parse(record[12].ToString()),
                                 usua_reg = record[13]?.ToString().Trim(),
                                 suc_pos = record[15]?.ToString().Trim(),
                                 doc_pos = record[16]?.ToString().Trim(),
                                 fech_pos = Convert.ToDateTime(record[19]),
                                 clave = record[20]?.ToString().Trim(),
                                 num_pos = Convert.ToInt32(record[18]),
                                 estado = Convert.ToInt32(record[14]),
                             };

                             bonodev_lst.Add(bonodevol);
                         }
                         catch (Exception ex)
                         {
                             Console.WriteLine($"⚠️ Error en fila {fila}: {ex.Message}");


                             continue; // o break si no quieres continuar
                         }
                     }
                 }

                 Procesos.InsertarBonoDevol(bonodev_lst);


                 break;*/

            case 8: //bono devolu catrelina

                using (FileStream fs = File.OpenRead(dbfFileBonodev))
                {
                    var reader = new DBFReader(fs);
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252); // ANSI típico de VFP

                    object[] record;
                    int fila = 0;

                    while (true)
                    {
                        try
                        {
                            record = reader.NextRecord();
                            if (record == null) break;

                            fila++;

                            var bonodevol = new t_bonodev
                            {

                                fecha = Convert.ToDateTime(record[0]),
                                super = record[1]?.ToString().Trim(),
                                num_bono = Convert.ToInt32(record[2]),
                                valorbono = Convert.ToInt32(record[3]),
                                terceroLong = Convert.ToInt64(record[5]),
                                suc_dev = record[6]?.ToString().Trim(),
                                doc_dev = record[7]?.ToString().Trim(),
                                cons_dev = record[8]?.ToString().Trim(),
                                num_dev = Convert.ToInt32(record[9]),
                                concepto = record[10]?.ToString().Trim(),
                                fech_reg = Convert.ToDateTime(record[11]),
                                hora_reg = TimeOnly.Parse(record[12].ToString()),
                                usua_reg = record[13]?.ToString().Trim(),
                                suc_pos = record[15]?.ToString().Trim(),
                                doc_pos = record[16]?.ToString().Trim(),
                                //fech_pos = Convert.ToDateTime(record[19]),
                                fech_pos = DateTime.TryParse(record[19]?.ToString(), out var f11) ? f11 : DateTime.MinValue,
                                clave = record[20]?.ToString().Trim(),
                                num_pos = Convert.ToInt32(record[18]),
                                estado = Convert.ToInt32(record[14]),
                            };

                            bonodev_lst.Add(bonodevol);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error en fila {fila}: {ex.Message}");


                            continue; // o break si no quieres continuar
                        }
                    }
                }

               await Procesos.InsertarBonoDevol(bonodev_lst);


                break;



            case 9:   //fenalpag

                using (FileStream fc = File.OpenRead(dbfFenalpag))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var fenalpag = new t_fenalpag
                        {
                            Tdoc = record[1]?.ToString()?.Trim(),
                            Tipo_per = record[2]?.ToString()?.Trim(),
                            Nom1 = record[3]?.ToString()?.Trim(),
                            Nom2 = record[4]?.ToString()?.Trim(),
                            Apl1 = record[5]?.ToString()?.Trim(),
                            Apl2 = record[6]?.ToString()?.Trim(),
                            Sexo = record[7]?.ToString()?.Trim(),
                            fech_nac = Convert.ToDateTime(record[8]),
                            Direcc = record[9]?.ToString()?.Trim(),
                            Ciudad = record[10]?.ToString()?.Trim(),
                            Tel = record[12]?.ToString()?.Trim(),
                            Emailfe1 = record[13]?.ToString()?.Trim(),
                            Anexo = Convert.ToInt32(record[0]),
                            Usuario = record[34]?.ToString()?.Trim(),
                            Usado = Convert.ToInt32(record[39]),
                            Nombre = record[11]?.ToString()?.Trim(),
                            Cod_sis = record[60]?.ToString()?.Trim(),
                            Autoriz = record[37]?.ToString()?.Trim(),

                        };

                        fenal_list.Add(fenalpag);

                    }
                }

                using (FileStream fc = File.OpenRead(dbfSisliqcre))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var sisliqcre = new t_sisliqcre
                        {
                            cod_sis = record[0]?.ToString()?.Trim(),
                            nombre = record[1]?.ToString()?.Trim(),
                            estado = ParseInt(record[2]),
                            cuotamin = ParseInt(record[3]),
                            cuotamax = ParseInt(record[4]),
                            tasamin = ParseInt(record[5]),
                            valmin = ParseInt(record[6]),
                            valmax = ParseInt(record[7]),
                            ncredmax = ParseInt(record[8]),
                            nreporte = record[9]?.ToString()?.Trim(),
                            fpago = record[10]?.ToString()?.Trim(),
                            factcom = ParseInt(record[11]),
                            fivacomi = ParseInt(record[12]),
                            actcobfi = ParseInt(record[13]),
                            cuo_fina = ParseInt(record[14]),
                            tercero = ParseInt(record[15]),
                            cuotami = ParseInt(record[16]),
                            cta_deb = record[17]?.ToString()?.Trim(),
                            cta_cred = record[18]?.ToString()?.Trim(),
                            codeudor = ParseInt(record[19]),
                            si_siscr = ParseInt(record[20]),
                            sistcred = record[21]?.ToString()?.Trim(),
                            tip_liq = ParseInt(record[22]),
                            iva_gen = ParseInt(record[23]),
                            vrcheque = ParseInt(record[24]),
                            vr_comisop = ParseInt(record[25]),
                            ivacomisop = ParseInt(record[26]),
                            repoliq = record[27]?.ToString()?.Trim(),
                            cta_intm = record[28]?.ToString()?.Trim(),
                            desnoemp = ParseInt(record[29]),
                            doc_omis = record[30]?.ToString()?.Trim(),
                            tcuotfij = ParseInt(record[31]),
                            nocausaint = ParseInt(record[32]),
                            nocredemp = ParseInt(record[33]),
                        };

                        sisliqcre_list.Add(sisliqcre);

                    }
                }

                //await Procesos.InsertarConfigpos(config5_list, config_list, xCodPos);
                await Procesos.InsertarFenalpag(fenal_list, sisliqcre_list);

                break;
            case 10:

                using (FileStream fc = File.OpenRead(dbfMovnom))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var movnom = new t_movnom
                        {

                            Empleado = record[4]?.ToString()?.Trim(),
                            Cedula = record[5]?.ToString()?.Trim(),
                            Apellido1 = record[9]?.ToString()?.Trim(),
                            Apellido2 = record[10]?.ToString()?.Trim(),
                            Nombre1 = record[7]?.ToString()?.Trim(),
                            Nombre2 = record[8]?.ToString()?.Trim(),
                            Concepto = record[18]?.ToString()?.Trim(),
                            Periodo = record[31]?.ToString()?.Trim(),
                            fecha = Convert.ToDateTime(record[28]),
                            Devengado = Convert.ToInt32(record[23]),
                            Descuento = Convert.ToInt32(record[24]),

                        };

                        movnomList.Add(movnom);

                    }
                }

                Procesos.InsertarMovnom(movnomList);


                break;

            case 11:

                using (FileStream fc = File.OpenRead(dbfPlanDet))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var planDet = new t_planDet
                        {

                            Fecha = DateOnly.FromDateTime(Convert.ToDateTime(record[3].ToString())),
                            Empleado = record[2]?.ToString()?.Trim(),
                            FechaCreacion = Convert.ToDateTime(record[4]),
                            Usa_crea = record[5]?.ToString()?.Trim(),
                            NumeroPlanilla = Convert.ToInt32(record[0]),
                        };

                        PlanDetList.Add(planDet);

                    }
                }

               await Procesos.InsertarPlanDet(PlanDetList);


                break;

            case 12:

                using (FileStream fc = File.OpenRead(dbfDcom))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var Sdcom = new t_dcom
                        {

                            ordenedecompra = record[0]?.ToString()?.Trim(),
                            producto = record[5]?.ToString()?.Trim(),
                            cantidad = Convert.ToInt32(record[19]),
                            valor = Convert.ToInt32(record[18]),
                            fechahoramovimiento = Convert.ToDateTime(record[26]),
                            descuento = Convert.ToInt32(record[35]),
                            sugerido = Convert.ToInt32(record[37]),
                            requerido = Convert.ToInt32(record[36]),
                            bonificacion = Convert.ToInt32(record[31]),
                            observacion = record[24]?.ToString()?.Trim(),
                            sucursal = record[1]?.ToString()?.Trim(),
                            diasinventario = Convert.ToInt32(record[17]),
                            iva = Convert.ToInt32(record[21]),
                            impuestoconsumo = Convert.ToInt32(record[55]),
                            talla = record[57]?.ToString()?.Trim(),
                            nombreproducto = record[6]?.ToString()?.Trim(),
                            subgruposproducto = record[7]?.ToString()?.Trim(),
                            marcasproducto = record[8]?.ToString()?.Trim(),
                            tipoProducto = record[9]?.ToString()?.Trim(),
                            valoriva = Convert.ToInt32(record[41]),
                            valorimpoconsumo = Convert.ToInt32(record[56]),
                            ean8 = record[10]?.ToString()?.Trim(),
                            ean13 = record[11]?.ToString()?.Trim(),
                            codigoreferencia = record[12]?.ToString()?.Trim(),

                        };

                        DcomList.Add(Sdcom);

                    }
                }

                Procesos.InsertarDcom(DcomList);

                break;
            case 13:

                using (FileStream fc = File.OpenRead(dbfFenalpag))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var sCreditos = new t_fenalCredtos
                        {

                            valormercanciacliente = Convert.ToInt32(record[30]),
                            cantidadcuotascliente = Convert.ToInt32(record[17]),
                            fenalpagid = Convert.ToInt32(record[0]),
                            tasainteres = Convert.ToInt32(record[16]),
                            iva = Convert.ToInt32(record[24]),
                            porcentajecuotainicial = Convert.ToInt32(record[19]),
                            numerocuotas = Convert.ToInt32(record[17]),
                            idformapago = record[48]?.ToString()?.Trim(),
                            porcentajeaval = Convert.ToInt32(record[55]),
                            valorcuotainicialcliente = Convert.ToInt32(record[18]),
                            porcentajecuotainicialcliente = Convert.ToInt32(record[19]),
                            periodicidad = Convert.ToInt32(record[38]),


                        };

                        creditosList.Add(sCreditos);

                    }
                }

                Procesos.InsertarCreditos(creditosList);

                break;

            case 14:

                int recordIndex = 0;
                using (FileStream fc = File.OpenRead(dbfCabFact))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {


                        try
                        {
                            var sCabFact = new t_cabFact
                            {

                                fechahoramovimiento = Convert.ToDateTime(record[1]),
                                sucursalid = record[0]?.ToString()?.Trim(),
                                cajacodigo = record[11]?.ToString()?.Trim(),
                                usuarioid = record[17]?.ToString()?.Trim(),
                                usuarionombre = record[18]?.ToString()?.Trim(),
                                terceroid = long.TryParse(record[7]?.ToString(), out var temp) ? temp : 0,
                                nombretercero = record[8]?.ToString()?.Trim(),
                                tarjetamercapesosid = long.TryParse(record[49]?.ToString(), out var temp3) ? temp3 : 0,
                                tarjetamercapesoscodigo = record[56]?.ToString()?.Trim(), // codigo de la tarjeta
                                documentofactura = record[2]?.ToString()?.Trim(),
                                totalventa = Convert.ToInt32(record[103]),
                                valorcambio = Convert.ToInt32(record[13]),
                                numerofactura = Convert.ToInt32(record[4]),
                                documentoid = record[107]?.ToString()?.Trim(),
                                documentonombre = record[3]?.ToString()?.Trim(),
                                identificacion = long.TryParse(record[7]?.ToString(), out var temp2) ? temp2 : 0,
                                tipopagoid = record[109]?.ToString()?.Trim(),
                                idcaja = record[110]?.ToString()?.Trim(),


                            };

                            cabfactList.Add(sCabFact);
                        }
                        catch (Exception ex)
                        {

                            Console.WriteLine(ex.ToString());
                        }




                        //creditosList.Add(sCabFact);

                    }
                }

                Console.WriteLine("Ingreso al proceso");

                await Procesos.InsertarCajasMovimientos(cabfactList);
                break;
            case 15:

                using (FileStream fc = File.OpenRead(dbfDetFact))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var sDetFact = new t_detFact
                        {

                            cajasmovimientosid = Convert.ToInt32(record[3]),
                            productoid = record[4]?.ToString()?.Trim(),
                            productonombre = record[5]?.ToString()?.Trim(),
                            productovalorsiniva = Convert.ToInt32(record[14]),
                            productoporcentajeiva = Convert.ToInt32(record[16]),
                            productovaloriva = Convert.ToInt32(record[15]),
                            productoexento = Convert.ToInt32(record[34]),
                            promocionid = record[35]?.ToString()?.Trim(),
                            promocionnombre = record[65]?.ToString()?.Trim(),
                            promocionvalordscto = Convert.ToInt32(record[39]),
                            productovalorimptoconsumo = Convert.ToInt32(record[36]),
                            vendedorid = record[8]?.ToString()?.Trim(),
                            valordescuentogeneral = Convert.ToInt32(record[39]),
                            totalitem = Convert.ToInt32(record[33]),
                            productoidmarca = record[23]?.ToString()?.Trim(),
                            productoidlinea = record[20]?.ToString()?.Trim(),
                            productoidsublinea = record[21]?.ToString()?.Trim(),
                            //public int valoracumulartarejetamercapesos { get; set; }
                            cantidaddelproducto = Convert.ToInt32(record[9]),
                            productovalorantesdscto = Convert.ToInt32(record[38]),
                            productoporcentajedscto = Convert.ToInt32(record[66]),
                            productovalordescuento = Convert.ToInt32(record[39]),
                            factorimptoconsumo = Convert.ToInt32(record[41]),
                            productonombremarca = record[67]?.ToString()?.Trim(),

                            documentofactura = record[2]?.ToString()?.Trim(),

                        };



                        detfactList.Add(sDetFact);

                    }
                }

               await Procesos.InsertarCajasDetalles(detfactList);


                break;

            case 16:   // esta tabla no se usa

                using (FileStream fc = File.OpenRead(dbfFileinc_desc))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;
                    var codigosIgnorados = new List<string> { "R1", ".02", ".R0", "S0", "R0", "R06", "R23", "R36", "R43", "R53", "S01", ".S0" };

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        // Validar que cod_ent (record[2]) y empleado (record[9]) no estén vacíos
                        string codent = record[2]?.ToString()?.Trim();
                        string empleado = record[9]?.ToString()?.Trim();

                        if (string.IsNullOrWhiteSpace(codent) || string.IsNullOrWhiteSpace(empleado))
                        {
                            continue; // Saltar si están vacíos
                        }

                        if (codigosIgnorados.Contains(codent))
                        {
                            continue; // Salta esta iteración si el código está en la lista
                        }

                        // Validar fechas
                        if (!DateTime.TryParse(record[11]?.ToString(), out DateTime fechaIni))
                        {
                            continue; // Saltar si la fecha inicial no es válida
                        }

                        if (!DateTime.TryParse(record[12]?.ToString(), out DateTime fechaFin))
                        {
                            continue; // Saltar si la fecha final no es válida
                        }

                        var IncapaNomina = new t_inc_desc
                        {
                            periodo = record[0]?.ToString()?.Trim(), //periodo
                            entidad = Convert.ToInt32(record[1]),//tipo
                            solo_arp = Convert.ToInt32(record[13]),//descuentodiasarp
                            cod_ent = codent, //entidadid
                            empleado = empleado, //empleadoid
                            fecha_ini = fechaIni,//fechainicio
                            fecha_fin = fechaFin,//fechafinal
                            vr_inc_sal = Convert.ToInt32(record[3]), //valorincapacidadsalud
                            vr_inc_arp = Convert.ToInt32(record[4]),//valorincapacidadriesgos
                            vr_inc_mat = Convert.ToInt32(record[5]), //valorlicenciamaternidad
                            au_inc_sal = record[6]?.ToString()?.Trim(), //valorincapacidadsaludautorizacion
                            au_inc_arp = record[7]?.ToString()?.Trim(),//valorincapacidadriesgosautorizacion
                            au_inc_mat = record[8]?.ToString()?.Trim(), //valorlicenciamaternidadautorizacion
                            apor_pag = Convert.ToInt32(record[10]), //aportespagadosaotrossubsistemas
                        };
                        inc_desc_list.Add(IncapaNomina);
                    }
                }
                Procesos.InsertarIncapacidadesNomina(inc_desc_list);


                break;

            case 17:

                
                using (FileStream fc = File.OpenRead(dbfTercero))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var value = GetValueOrDefault<string>(record, 1, "0"); // Obtener como string

                        var sTerceros = new t_terceros
                        {



                            TipoPersona = GetValueOrDefault<string>(record, 109, null),
                            IdTipoIdentificacion = Convert.ToInt32(record[82]),
                            Identificacion = long.TryParse(record[1]?.ToString(), out var temp) ? temp : 0,
                            Nombre1 = GetValueOrDefault<string>(record, 85, null),
                            Nombre2 = GetValueOrDefault<string>(record, 86, null),
                            Apellido1 = GetValueOrDefault<string>(record, 83, null),
                            Apellido2 = GetValueOrDefault<string>(record, 841, null),
                            Genero = GetValueOrDefault<string>(record, 110, null),
                            FechaNacimiento = GetValueOrDefault<DateTime>(record, 111, DateTime.MinValue),
                            RazonSocial = GetValueOrDefault<string>(record, 2, null),
                            NombreComercial = GetValueOrDefault<string>(record, 58, null),
                            Direccion = GetValueOrDefault<string>(record, 5, null),
                            Email = GetValueOrDefault<string>(record, 87, null),
                            Email2 = GetValueOrDefault<string>(record, 144, null),
                            IdDepartamento = GetValueOrDefault<int>(record, 3, 0),
                            IdMunicipio = GetValueOrDefault<int>(record, 3, 0),
                            Telefono1 = GetValueOrDefault<string>(record, 7, null),
                            Telefono2 = GetValueOrDefault<string>(record, 7, null),
                            Estado = GetValueOrDefault<int>(record, 12, 0),
                            EsCliente = GetValueOrDefault<int>(record, 13, 0),
                            EsEmpleado = GetValueOrDefault<int>(record, 91, 0),
                            EsPasante = GetValueOrDefault<int>(record, 93, 0),
                            EsProveedor = GetValueOrDefault<int>(record, 22, 0),
                            FechaCreacion = GetValueOrDefault<DateTime>(record, 11, DateTime.MinValue),
                            FechaActualizacion = GetValueOrDefault<DateTime>(record, 112, DateTime.MinValue),
                            DiasCredito = GetValueOrDefault<int>(record, 21, 0),
                            EsProveedorFruver = GetValueOrDefault<int>(record, 88, 0),
                            EsProveedorBolsaAgropecuaria = GetValueOrDefault<int>(record, 89, 0),
                            EsProveedorCampesinoDirecto = GetValueOrDefault<int>(record, 90, 0),
                            EsProveedorRestaurante = GetValueOrDefault<int>(record, 97, 0),
                            EsProveedorPanaderia = GetValueOrDefault<int>(record, 98, 0),
                            EsOtroTipo = GetValueOrDefault<int>(record, 92, 0),
                            EsGasto = GetValueOrDefault<int>(record, 94, 0),
                            CotizaEPS = GetValueOrDefault<int>(record, 127, 0),
                            CotizaFondoPension = GetValueOrDefault<int>(record, 128, 0),
                            CotizaARP = GetValueOrDefault<int>(record, 129, 0),
                            TarifaARP = GetValueOrDefault<int>(record, 130, 0),
                            RegimenSimplificado = GetValueOrDefault<int>(record, 95, 0),
                            NoPracticarRetFuente = GetValueOrDefault<int>(record, 63, 0),
                            NoPracticarRetIVA = GetValueOrDefault<int>(record, 64, 0),
                            Autorretenedor = GetValueOrDefault<int>(record, 24, 0),
                            EsRetenedorFuente = GetValueOrDefault<int>(record, 62, 0),
                            DescontarAsohofrucol = GetValueOrDefault<int>(record, 60, 0),
                            AsumirImpuestos = GetValueOrDefault<int>(record, 70, 0),
                            RetenerFenalce = GetValueOrDefault<int>(record, 71, 0),
                            AsumirFenalce = GetValueOrDefault<int>(record, 72, 0),
                            BolsaAgropecuaria = GetValueOrDefault<int>(record, 73, 0),
                            RegimenComun = GetValueOrDefault<int>(record, 28, 0),
                            RetenerSiempre = GetValueOrDefault<int>(record, 25, 0),
                            GranContribuyente = GetValueOrDefault<int>(record, 26, 0),
                            AutorretenedorIVA = GetValueOrDefault<int>(record, 27, 0),
                            IdCxPagar = GetValueOrDefault<int>(record, 50, 0),
                            DeclaranteRenta = GetValueOrDefault<int>(record, 116, 0),
                            DescuentoNIIF = GetValueOrDefault<int>(record, 132, 0),
                            DescontarFNFP = GetValueOrDefault<int>(record, 133, 0),
                            ManejaIVAProductoBonificado = GetValueOrDefault<int>(record, 143, 0),
                            ReteIVALey560_2020 = GetValueOrDefault<int>(record, 51, 0),
                            RegimenSimpleTributacion = GetValueOrDefault<int>(record, 151, 0),
                            TipoDescuentoFinanciero = GetValueOrDefault<int>(record, 136, 0),
                            Porcentaje1 = GetValueOrDefault<int>(record, 138, 0),
                            Porcentaje2 = GetValueOrDefault<int>(record, 139, 0),
                            Porcentaje3 = GetValueOrDefault<int>(record, 140, 0),
                            Porcentaje4 = GetValueOrDefault<int>(record, 141, 0),
                            Porcentaje5 = GetValueOrDefault<int>(record, 142, 0),
                            IdICATerceroCiudad = GetValueOrDefault<int>(record, 135, 0),
                            EstadoRUT = GetValueOrDefault<int>(record, 77, 0),
                            FechaRut = GetValueOrDefault<DateTime>(record, 78, DateTime.MinValue),
                            IdCxCobrar = GetValueOrDefault<int>(record, 49, 0),
                            DigitoVerificacion = GetValueOrDefault<string>(record, 4, null),
                            IdEmpleado = GetValueOrDefault<int>(record, 1, 0),
                            BaseDecreciente = GetValueOrDefault<int>(record, 137, 0),
                            IdResponsabilidadesFiscales = GetValueOrDefault<string>(record, 165, null),
                            IdResponsabilidadesTributarias = GetValueOrDefault<string>(record, 167, null),
                            IdUbicacionDANE = GetValueOrDefault<string>(record, 74, null),
                            CodigoPostal = GetValueOrDefault<string>(record, 164, null),

                        };

                        tercero_list.Add(sTerceros);
                    }
                }


                using (FileStream fc = File.OpenRead(dbfFileCiudad))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var ciudad = new t_ciudad
                        {
                            Municipio = record[1]?.ToString()?.Trim(),
                            Departamento = record[2]?.ToString()?.Trim(),
                            Dane = record[3]?.ToString()?.Trim(),

                        };

                        ciudade_lis.Add(ciudad);

                    }
                }



                await Procesos.insertarTerceros(tercero_list, ciudade_lis);

                break;


            case 18: // cajasmovimientosformaspago


                using (FileStream fc = File.OpenRead(dbfFpagos))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var fpago = new t_fpago
                        {
                            cajasmovimientosid = record[2]?.ToString()?.Trim(),
                            formadepagoId = record[16]?.ToString()?.Trim(),
                            valor = Convert.ToInt32(record[17]),
                            nombreFpago = record[18]?.ToString()?.Trim(),
                            codigoDatafon = record[20]?.ToString()?.Trim(),
                            redencion = record[3]?.ToString()?.Trim(),
                            numero = Convert.ToInt32(record[4]),


                        };

                        fpago_list.Add(fpago);

                    }
                }


                await Procesos.insertarFpago(fpago_list);
                break;

            case 19:

                using (FileStream fc = File.OpenRead(dbfdataf))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var dataf = new t_dataf
                        {

                            cajasmovimientosid = Convert.ToInt32(record[5]),
                            fechayhorasolicitud = Convert.ToDateTime(record[1]),
                            solicitudoperacion = record[16]?.ToString()?.Trim(),
                            solicitudmonto = Convert.ToInt32(record[12]),
                            solicitudiva = Convert.ToInt32(record[13]),
                            solicitudcodigocajero = record[6]?.ToString()?.Trim(),
                            respuestarespuesta = record[7]?.ToString()?.Trim(),
                            respuestaautorizacion = record[8]?.ToString()?.Trim(),
                            respuestatarjeta = record[9]?.ToString()?.Trim(),
                            respuestatipotarjeta = record[10]?.ToString()?.Trim(),
                            respuestafranquicia = record[11]?.ToString()?.Trim(),
                            respuestamonto = Convert.ToInt32(record[12]),
                            respuestaiva = Convert.ToInt32(record[13]),
                            respuestarecibo = record[14]?.ToString()?.Trim(),
                            respuestacuotas = record[15]?.ToString()?.Trim(),
                            respuestarrn = record[18]?.ToString()?.Trim(),
                            respuestaobservacion = record[19]?.ToString()?.Trim(),

                        };
                        //dataf_list
                        dataf_list.Add(dataf);

                    }
                }

               await Procesos.insertarDataf(dataf_list);

                break;
            case 20:

                using (FileStream fc = File.OpenRead(dbfSeparados))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var separados = new t_separados
                        {

                            terceroid = Convert.ToInt32(record[7]),
                            fechayhoraconsumo = Convert.ToDateTime(record[13]),
                            usuariocreaid = record[4]?.ToString()?.Trim(),
                            fechahoraregistro = Convert.ToDateTime(record[3]),
                            cajasmovimientosid = Convert.ToInt32(record[12]),
                            sucursalid = record[0]?.ToString()?.Trim(),

                            documentofactura = record[11]?.ToString()?.Trim(),


                        };

                        separados_list.Add(separados);

                    }
                }


                Procesos.insertarSeparados(separados_list);



                break;

            case 21:

                using ( FileStream fc = File.OpenRead(dbfCuotas))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var cuotas = new t_cuotas
                        {



                            fenalpagid = Convert.ToInt32(record[6]),
                            cuotanumero = record[15]?.ToString()?.Trim(),
                            valorcuota = Convert.ToInt32(record[16]),
                            fechavencimientocuota = Convert.ToDateTime(record[9]),
                            fechahorapago = Convert.ToDateTime(record[1]),
                            //idusuariopago = record[4]?.ToString()?.Trim(),
                            cajasmovimientosid = Convert.ToInt32(record[3]),
                            sucursalid = record[0]?.ToString()?.Trim(),
                            documentofactura = record[2]?.ToString()?.Trim(),
                            valorabono = Convert.ToInt32(record[7]),

                        };

                        cuotas_list.Add(cuotas);

                    }
                }

                await Procesos.InsertarCuotas(cuotas_list);


                break;

            case 22: // tablas de creditos

                using (FileStream fc = File.OpenRead(dbfcredFin))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var cuotas = new t_cred2
                        {

                            documentoreferencia = record[14]?.ToString()?.Trim(),
                            fechahoracredito = Convert.ToDateTime(record[1]),
                            sucursalid = record[0]?.ToString()?.Trim(),
                            numerocuenta = record[5]?.ToString()?.Trim(),
                            documentoaf = record[11]?.ToString()?.Trim(),
                            tipoabono = record[17]?.ToString()?.Trim(),
                            totalcredito = Convert.ToInt32(record[23]),
                            consecutivoaf = record[21]?.ToString()?.Trim(),
                            cuotafija = record[20]?.ToString()?.Trim(),
                            amortizado = record[25]?.ToString()?.Trim(),


                        };

                        cred_list.Add(cuotas);

                    }
                }

                await Procesos.InsertarCredit(cred_list);



                break;

            case 23:


                using (FileStream fc = File.OpenRead(dbfcred2))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var cuotas = new t_credCuotas
                        {

                            creditosid = record[12]?.ToString()?.Trim(),
                            numerocuota = record[13]?.ToString()?.Trim(),
                            valorcuota = Convert.ToInt32(record[8]),
                            pagada = record[27]?.ToString()?.Trim(),

                        };

                        cCuota_list.Add(cuotas);

                    }
                }

                await Procesos.InsertarCuota2(cCuota_list);



                break;
            case 24: //no terminado - traslados


                using (FileStream fc = File.OpenRead(dbfdetras))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int rowIndex = 0;


                

                while ((record = reader.NextRecord()) != null)
                    {
                        rowIndex++;
                        try
                        {

                            string cantidadRaw = record[5]?.ToString()?.Trim();
                            string cantidadRaw2 = record[6]?.ToString()?.Trim();

                            var detras = new t_despacho
                            {
                                IdSucursal = record[0]?.ToString()?.Trim(),
                                IdDocumento = record[1]?.ToString()?.Trim(),
                                Consecutivo = record[2]?.ToString()?.Trim(),

                                Numero = SafeToInt(record[3], "Numero", rowIndex),
                                IdProducto = record[4]?.ToString()?.Trim(),
                                CantidadReg = SafeToDecimalStrict(cantidadRaw, "CantidadReg", rowIndex),
                                CantidadEnt = SafeToDecimalStrict(cantidadRaw, "CantidadEnt", rowIndex),
                                Fecha = SafeToDate(record[7], "Fecha", rowIndex),
                                IdTercero = SafeToInt(record[9], "IdTercero", rowIndex),

                                Despacho = SafeToInt(record[10], "Numero", rowIndex),
                                TipoProceso = record[11]?.ToString()?.Trim(),
                                Observacion = record[12]?.ToString()?.Trim(),
                                DirecEntrega = record[13]?.ToString()?.Trim(),
                                TelefEntrega = record[14]?.ToString()?.Trim(),

                                IdSucursalTras = record[15]?.ToString()?.Trim(),
                                DocumentoTras = record[16]?.ToString()?.Trim(),
                                ConsecutivoTras = record[17]?.ToString()?.Trim(),
                                NumeroTras = SafeToInt(record[18], "NumeroTras", rowIndex),
                                FechaDespacho = SafeToDate(record[19], "FechaDespacho", rowIndex),
                                IdUsuarioDespacho = record[21]?.ToString()?.Trim(),

                                Estado = SafeToInt(record[22], "Numero", rowIndex),

                                IdJornada = SafeToInt(record[25], "Numero", rowIndex),
                                CodigoAut = SafeToInt(record[26], "NumeroTras", rowIndex),
                                IdDomicilio = SafeToInt(record[27], "NumeroTras", rowIndex),
                                TipoVenta = record[28]?.ToString()?.Trim(),
                                Impreso = SafeToInt(record[29], "NumeroTras", rowIndex),
                                Referencia = record[30]?.ToString()?.Trim(),
                                Entrega = SafeToInt(record[31], "NumeroTras", rowIndex),
                                Observacion2 = record[32]?.ToString()?.Trim(),

                                FechaEn = SafeToDate(record[4], "FechaEn", rowIndex),

                                IdZona = record[34]?.ToString()?.Trim(),

                                Pendiente = SafeToInt(record[36], "NumeroTras", rowIndex),
                                FechaEntrega = SafeToDate(record[33], "FechaEntrega", rowIndex),
                                IdSucursalDes = record[35]?.ToString()?.Trim(),
                                IdBodegaDesp = record[38]?.ToString()?.Trim(),
                            };

                            detras_list.Add(detras);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error en registro {rowIndex}: {ex.Message}");
                            break; // Opcional: detenerse en el primer error
                        }
                    }
                }

                await Procesos.InsertarDetras(detras_list);

                bool IsValidNumber(string value)
                {
                    // Verificar si el valor es numérico
                    return int.TryParse(value, out _);
                }




        decimal SafeToDecimalStrict(string val, string fieldName, int rowIndex)
                {
                    if (string.IsNullOrWhiteSpace(val))
                    {
                        throw new FormatException($"Campo {fieldName} en fila {rowIndex} está vacío");
                    }

                    if (!decimal.TryParse(val, out var result))
                    {
                        throw new FormatException($"No se pudo convertir '{val}' a decimal en campo {fieldName}, fila {rowIndex}");
                    }

                    return result;
                }


                int SafeToInt(object val, string fieldName, int rowIndex)
                {
                    if (val == null) return 0;

                    var str = val.ToString().Trim();
                    if (string.IsNullOrEmpty(str) || str.Contains("*"))
                    {
                        Console.WriteLine($"⚠️ Valor inválido en campo {fieldName}, fila {rowIndex}: {val}");
                        return 0;
                    }

                    return int.TryParse(str, out var result) ? result : 0;
                }

                decimal SafeToDecimal(object val, string fieldName, int rowIndex)
                {
                    if (val == null) return 0m;

                    var str = val.ToString().Trim();
                    if (string.IsNullOrEmpty(str) || str.Contains("*"))
                    {
                        Console.WriteLine($"⚠️ Valor inválido en campo {fieldName}, fila {rowIndex}: {val}");
                        return 0m;
                    }

                    return decimal.TryParse(str, out var result) ? result : 0m;
                }

                DateTime SafeToDate(object val, string fieldName, int rowIndex)
                {
                    if (val == null) return DateTime.MinValue;

                    var str = val.ToString().Trim();
                    if (string.IsNullOrEmpty(str) || str.Contains("*"))
                    {
                        Console.WriteLine($"⚠️ Fecha inválida en campo {fieldName}, fila {rowIndex}: {val}");
                        return DateTime.MinValue;
                    }

                    return DateTime.TryParse(str, out var result) ? result : DateTime.MinValue;
                }



                break;


            case 25:

                using (FileStream fc = File.OpenRead(dbfMpmovi))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var mpmovi = new t_mpmovi
                        {
                            cedula = Convert.ToInt64(record[11]),
                            fecha = Convert.ToDateTime(record[2]),
                            sucursal = record[0]?.ToString()?.Trim(),
                            entra = Convert.ToInt64(record[4]),
                        };
                        mpmovi_list.Add(mpmovi);
                    }
                }
                Procesos.InsertarMoviMecapesos(mpmovi_list);


                break;

                //mercapesos
            case 26:


                using (FileStream fc = File.OpenRead(dbfMercapesos))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var merca = new t_mercapesos
                            {
                                IdTercero = ParseLong(record[7]),
                                Codigo = ParseLong(record[0]),
                                Tarjeta = record[1]?.ToString()?.Trim(),
                                Acumulado = ParseInt(record[2]),
                                Observaciones = record[18]?.ToString()?.Trim(),
                                Estado = ParseInt(record[19]),
                                Aceptacion = record[8]?.ToString()?.Trim(),
                                FechaCreacion = ParseDate(record[12]),
                            };

                            merca_list.Add(merca);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }
                await Procesos.InsertarMercapesos(merca_list);

                long ParseLong(object value)
                {
                    if (value == null) return 0;
                    var str = value.ToString().Trim();
                    if (string.IsNullOrEmpty(str)) return 0;
                    if (long.TryParse(str, out var result)) return result;
                    return 0; // o lanza excepción controlada
                }

                int ParseInt(object value)
                {
                    if (value == null) return 0;
                    var str = value.ToString().Trim();
                    if (string.IsNullOrEmpty(str)) return 0;
                    if (int.TryParse(str, out var result)) return result;
                    return 0;
                }

                DateTime ParseDate(object value)
                {
                    if (value == null) return DateTime.MinValue;
                    if (DateTime.TryParse(value.ToString(), out var dt)) return dt;
                    return DateTime.MinValue;
                }


            break;

            case 27:

                // tabla de cabdoc
                using (FileStream fc = File.OpenRead(cabdoc))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var cabecera = new t_cabdoc
                            {
                                usuario = record[14]?.ToString()?.Trim(),
                                docum   = record[2]?.ToString()?.Trim(),
                                numero  = ParseInt(record[3]),
                                consecut = record[30]?.ToString()?.Trim(),

                            };

                            cabdoc_list.Add(cabecera);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }



                // tabla de cod_ret
                using (FileStream fc = File.OpenRead(codigoscuen))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var rete = new t_cuen
                            {
                                cod_ret  = record[0]?.ToString()?.Trim(),
                                nombre   = record[1]?.ToString()?.Trim(),
                                cuenta   = record[2]?.ToString()?.Trim(),
                                fact_ret = ParseInt(record[3]),
                                
                            };

                            cuen_list.Add(rete);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }


                using (FileStream fc = File.OpenRead(dbfMovite))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var movite = new t_mov_inventarios
                            {


                                IdSucursal = record[0]?.ToString()?.Trim(),
                                IdDocumento = record[2]?.ToString()?.Trim(),
                                Fecha = ParseDate(record[1]),
                                Periodo = record[45]?.ToString()?.Trim(),
                                IdTercero =ParseInt(record[9]),
                                IdCxPagar =  record[37]?.ToString()?.Trim(),
                                //IdBanco = ParseInt(record[19]),
                                //Cheque = record[8]?.ToString()?.Trim(),
                                //FechaCheque = ParseDate(record[12]),
                                //IVAFletes = ParseInt(record[19]),
                                Zona = ParseInt(record[7]),
                                //Vendedor = record[8]?.ToString()?.Trim(),
                                //Observaciones = record[8]?.ToString()?.Trim(),
                                Numero = ParseInt(record[3]),
                                //FechaRecibido = ParseDate(record[12]),
                                

                            };

                            movite_list.Add(movite);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                await Procesos.InsertarMovite(movite_list, cuen_list, cabdoc_list); 

                break;

            case 28:

                using (FileStream fc = File.OpenRead(dbfDetalleMovite))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var Detmovite = new t_det_movInventario
                            {

                               IdMovimiento = ParseLong2(record[9]),
                               IdProducto = record[4]?.ToString()?.Trim(),
                               IdBodega = record[5]?.ToString()?.Trim(),
                               Cantidad = ParseLong2(record[12]),
                               Recibida = ParseLong2(record[12]),
                               TipoMov = record[0]?.ToString()?.Trim(),
                               CostoUnitarioBruto = ParseLong2(record[13]),
                              // PorcentajeDescuento = ParseInt(record[9]),
                               //Descuento = ParseInt(record[9]),
                               CostoUnitarioNeto = ParseLong2(record[13]),
                               CostoTotal = ParseLong2(record[13]),
                               PorcentajeIVA = ParseLong2(record[32]),
                               ValorIVA = ParseLong2(record[21]),
                               CostoTotalIVA = ParseLong2(record[21]),
                               ImpuestoConsumo = ParseLong2(record[41]),
                               //PorcentajeRentabilidad = ParseInt(record[9]),
                               FechaCreacion = ParseDate(record[1]),
                             //  Fletes = ParseInt(record[9]),
                              // Observaciones = record[0]?.ToString()?.Trim(),


                               // campos para busqueda 
                               numero = ParseInt(record[3]),
                               documento = record[2]?.ToString()?.Trim(),

                            };

                            Detmovite_list.Add(Detmovite);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                



                await Procesos.InsertarDetalleMovite(Detmovite_list);



                    long ParseLong2(object value)
                    {
                        if (value == null || value == DBNull.Value)
                            return 0;

                        if (value is long l) return l;
                        if (value is int i) return i;
                        if (value is decimal d) return (long)d;
                        if (value is double db) return (long)db;
                        if (long.TryParse(value.ToString(), out var result)) return result;

                        return 0;
                    }


            
                break;
            case 29:



                using (FileStream fc = File.OpenRead(cabdoc))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var cabecera = new t_cabdoc
                            {
                                usuario = record[14]?.ToString()?.Trim(),
                                docum = record[2]?.ToString()?.Trim(),
                                numero = ParseInt(record[3]),
                                consecut = record[30]?.ToString()?.Trim(),

                            };

                            cabdoc_list.Add(cabecera);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                // tabla de cod_ret
                using (FileStream fc = File.OpenRead(codigoscuen))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var rete = new t_cuen
                            {
                                cod_ret = record[0]?.ToString()?.Trim(),
                                nombre = record[1]?.ToString()?.Trim(),
                                cuenta = record[2]?.ToString()?.Trim(),
                                fact_ret = ParseInt(record[3]),

                            };

                            cuen_list.Add(rete);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfmovdoc))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var movdoc = new t_mov_contables
                            {

                                Sucursal = record[0]?.ToString()?.Trim(),
                                Documento = record[2]?.ToString()?.Trim(),
                                Fecha = ParseDate(record[1]),
                                Periodo = record[36]?.ToString()?.Trim(),
                                Tercero = ParseInt(record[5]),
                                CxPagar = record[24]?.ToString()?.Trim(),
                                Zona = record[13]?.ToString()?.Trim(),
                                Vendedor = record[12]?.ToString()?.Trim(),
                                Observaciones = record[9]?.ToString()?.Trim(),
                                Factura = record[18]?.ToString()?.Trim(),
                                IdUsuario = record[0]?.ToString()?.Trim(),
                                IdEstado = record[0]?.ToString()?.Trim(),
                                IdCaja = record[22]?.ToString()?.Trim(),
                                Concepto = record[9]?.ToString()?.Trim(),


                            };

                            movdoc_list.Add(movdoc);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }


                await Procesos.InsertarMovdoc(movdoc_list, cuen_list, cabdoc_list);
                


                break;

            case 30:

                using (FileStream fc = File.OpenRead(dbfdetalleMovdoc))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var Detmovdoc = new t_det_movContable
                            {

                                IdMovimiento = ParseLong2(record[9]),
                                Cuenta = record[4]?.ToString()?.Trim(),
                               // NombreCuenta = record[4]?.ToString()?.Trim(),
                                Base = ParseLong2(record[10]),
                                //Factor = ParseLong2(record[9]),
                                Debe = ParseCurrency(record[7]),
                                Haber = ParseCurrency(record[8]),
                               // Naturaleza = record[4]?.ToString()?.Trim(),
                                CodigoPPYE = ParseLong2(record[9]),
                                Anexo = record[11]?.ToString()?.Trim(),
                                DocReferencia = record[18]?.ToString()?.Trim(),
                                FechaVencimiento = ParseDate(record[14]),
                                IdCodigoICA =record[40]?.ToString()?.Trim(),
                                IdVendedor = record[12]?.ToString()?.Trim(),
                               // IdOperacion = ParseLong2(record[9]),
                                IdZona =record[13]?.ToString()?.Trim(),
                               // ClasePPYE = record[4]?.ToString()?.Trim(),
                               // Tipo = record[4]?.ToString()?.Trim(),
                                //IdFacturasProductos = ParseLong2(record[9]),
                                Documento = record[2]?.ToString()?.Trim(),
                                Sucursal = record[0]?.ToString()?.Trim(),
                                periodo  = record[36]?.ToString()?.Trim(),


                                Fecha = ParseDate(record[1]),


                            };

                            detMovdoc_list.Add(Detmovdoc);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                await Procesos.InsertarDetalleMovdoc(detMovdoc_list);
                break;

            case 31:


                using (FileStream fc = File.OpenRead(dbfInventarios))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var inventario = new t_inventario
                            {
                                IdSucursal = record[0]?.ToString()?.Trim(),
                                IdProducto = record[4]?.ToString()?.Trim(),
                                Cantidad   = ParseLong2(record[12]),
                                IdBodega   = record[5]?.ToString()?.Trim(),
                                FechaCorte = ParseDate(record[1]),

                            };

                            inventarios_list.Add(inventario);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                await Procesos.InsertarInventario(inventarios_list);
                break;
            case 32:


               var lista =  Utilidades.LeerYUnirTablas();

                var listaParaInsertar = lista.Select(r => new t_Ele_PPYE
                {
                    IdCategoria = r.CatPpe,
                    scat_ppe = r.scat_ppe,
                    sbcat_ppe = r.sbcat_ppe,
                    CodCategoria = r.CatPpe,
                    Elemento = r.TipoElem,
                    Ref1 = r.Ref1Ppe,
                    Ref2 = r.Ref2Ppe,
                    IdGrupoContable = r.ModcPpe,
                    IdCuenta = r.CtaDepre,
                    CodAgrupacion = r.CodAgrup
                }).ToList();

                await Procesos.InsertarElementosPPYE(listaParaInsertar);


                break;
            case 33:


                await Procesos.InsertarSepDocumentos();

                break;
            case 34:

                await Procesos.InsertarOrdenCompraSucursal();
                break;

            case 35:


                var gradinsa = Utilidades.LeerYUnirTablas2();

                var listaInserCargue = gradinsa.Select(r => new t_planCargue
                {

                    IdPlanCargue = r.Despacho,
                    CantidadProducto =  r.CantReg,
                    IdRuta = r.Ruta,
                    IdConductor=  r.Conductor,
                    Observaciones =  r.Observac,
                    FechaSalida  = r.Fech_Sal,
                    IdPlaca = r.Placa,

                }).ToList();

                await Procesos.InsertarPlanCargue(listaInserCargue);

                break;
            case 36:


                using (FileStream fc = File.OpenRead(dbfModel_contable))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var model_contab = new t_modelContable
                            {
                               CodigoModelo = record[1]?.ToString()?.Trim(),
                               Nombre = record[4]?.ToString()?.Trim(),
                               Estado = record[0]?.ToString()?.Trim(),
                               CuentaGasto = record[2]?.ToString()?.Trim(),
                               CuentaDepreciacionAcumulada = record[3]?.ToString()?.Trim(),

                            };

                            modelConta_list.Add(model_contab);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                await Procesos.InsertarModeloContable(modelConta_list);

                break;

            case 37:


                using (FileStream fc = File.OpenRead(codigoscuen))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var rete = new t_cuen
                            {
                                cod_ret = record[0]?.ToString()?.Trim(),
                                nombre = record[1]?.ToString()?.Trim(),
                                cuenta = record[2]?.ToString()?.Trim(),
                                fact_ret = ParseInt(record[3]),

                            };

                            cuen_list.Add(rete);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }


                using (FileStream fc = File.OpenRead(dbfFacturas))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var fact_pro = new t_facturasPro
                            {

                                Numero=ParseInt(record[2]),
                                Fecha = ParseDate(record[4]),
                                IdProveedor = ParseInt(record[17]),
                                IdDocumento= record[1]?.ToString()?.Trim(),
                                
                                IdSucursal= record[0]?.ToString()?.Trim(),
                                IdOrdenCompra = record[32]?.ToString()?.Trim(),
                                Subtotal = ParseInt(record[30]),
                                Total = ParseInt(record[31]),
                                CxPagar = record[8]?.ToString()?.Trim(),
                                DiasCredito = ParseInt(record[7]),
                                FechaRecibido = ParseDate(record[1]),
                                Observaciones = record[33]?.ToString()?.Trim(),
                                
                                IdBodega = record[34]?.ToString()?.Trim(),
                                IdCentroCosto= record[35]?.ToString()?.Trim(),
                                Contado = ParseInt(record[6]),
                                Receptor = record[22]?.ToString()?.Trim(),
                                Entradas = ParseInt(record[19]),
                               // @CGrabado{contador},
                               // @CExcluido{contador},
                                IConsumo = ParseInt(record[23]),
                                Flete = ParseInt(record[21]),
                                ValorIVA = ParseInt(record[20]),
                                ValorRetencion = ParseInt(record[25]),
                                Fenaice = ParseInt(record[26]),
                                ReteICA = ParseInt(record[27]),
                                IdRFte = record[8]?.ToString()?.Trim(),
                                RFte = ParseInt(record[25]),
                                
                                Asohofrucol = ParseInt(record[28]),
                                FNFP = ParseInt(record[29]),

                                Periodo = record[3]?.ToString()?.Trim(),
                                
                                IdUsuarioElaboro= record[22]?.ToString()?.Trim(),
                                CxP = record[36]?.ToString()?.Trim(),


                            };

                            facturaPro_list.Add(fact_pro);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

  
                
                await Procesos.InsertarFacturaPro(facturaPro_list, cuen_list);

                break;
            case 38:

                using (FileStream fc = File.OpenRead(dbfConLiqFact))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var conliqf = new t_conliqf
                            {
                                codref = record[0]?.ToString()?.Trim(),
                                nombre = record[1]?.ToString()?.Trim(),
                                si_agrega = ParseInt(record[2]),
                                si_manual = ParseInt(record[3]),
                                si_suma = ParseInt(record[4]),
                                modifica = ParseInt(record[5]),
                                n_orden = record[6]?.ToString()?.Trim(),
                                n_grupo = record[7]?.ToString()?.Trim(),
                            };

                            conLiqf_list.Add(conliqf);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfDetLiqFact))
                {
                    var reader = new DBFReader(fc);
                    //reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var detliqf = new t_detliqf
                            {
                                codref = record[0]?.ToString()?.Trim(),
                                docum = record[1]?.ToString()?.Trim(),
                                cuenta = record[2]?.ToString()?.Trim(),
                                naturaleza = record[3]?.ToString()?.Trim(),
                                si_saldo = ParseInt(record[4]),
                                ex_cuenta = record[5]?.ToString()?.Trim(),

                            };

                            detLiqf_list.Add(detliqf);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                await Procesos.InsertarLiqFactItems(conLiqf_list, detLiqf_list);
                await Procesos.InsertarMLiqFact();
                break;

            case 39:

                int xCodPos = 0;
                Console.WriteLine("ingrese el numero de pos que desea migrar");
                xCodPos = int.Parse(Console.ReadLine());

                using (FileStream fc = File.OpenRead(dbfConfig5))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var config5 = new t_config5
                            {
                                dir_serv = record[0]?.ToString()?.Trim(),
                                fech_bpos = Convert.ToDateTime(record[1]),
                                bodega = record[2]?.ToString()?.Trim(),
                                cod_pos = record[3]?.ToString()?.Trim(),
                                empresa = ParseInt(record[4]),
                                crea_anex = ParseInt(record[5]),
                                sucursal = record[6]?.ToString()?.Trim(),
                                tipo_fecha = ParseInt(record[7]),
                                fecha_doc = Convert.ToDateTime(record[8]),
                                docum_fac = record[9]?.ToString()?.Trim(),
                                docum_dev = record[10]?.ToString()?.Trim(),
                                fact_act = ParseInt(record[11]),
                                devo_act = ParseInt(record[12]),
                                docum_otr = record[13]?.ToString()?.Trim(),
                                otros_act = ParseInt(record[14]),
                                mens1 = record[15]?.ToString()?.Trim(),
                                mens2 = record[16]?.ToString()?.Trim(),
                                transac = ParseInt(record[17]),
                                fec_inip = Convert.ToDateTime(record[18]),
                                fact_cxc = ParseInt(record[19]),
                                si_bajar = ParseInt(record[20]),
                                uavisofc = Convert.ToDateTime(record[21]),
                                uavisodv = Convert.ToDateTime(record[22]),
                                si_bodega = ParseInt(record[23]),
                                cerr_grab = ParseInt(record[24]),
                                interv_cie = ParseInt(record[25]),
                                preg_prec = ParseInt(record[26]),
                                pide_cant = ParseInt(record[27]),
                                pide_desc = ParseInt(record[28]),
                                si_descgen = ParseInt(record[29]),
                                fp_pred1 = record[30]?.ToString()?.Trim(),
                                fp_pred2 = record[31]?.ToString()?.Trim(),
                                ult_cierre = Convert.ToDateTime(record[32]),
                                sin_busi = ParseInt(record[33]),
                                ccosto = record[34]?.ToString()?.Trim(),
                                cod_cash = record[35]?.ToString()?.Trim(),
                                cod_papel = record[36]?.ToString()?.Trim(),
                                win_fil0 = ParseInt(record[37]),
                                win_col0 = ParseInt(record[38]),
                                precio_up = ParseInt(record[39]),
                                ayu_enter = ParseInt(record[40]),
                                fec_local = ParseInt(record[41]),
                                espac_tit1 = ParseInt(record[42]),
                                n_lineblan = ParseInt(record[43]),
                                ult_expor = Convert.ToDateTime(record[44]),
                                deci_puni = ParseInt(record[45]),
                                si_fac_dev = ParseInt(record[46]),
                                items_red = ParseInt(record[47]),
                                items_add = ParseInt(record[48]),
                                win_exit = ParseInt(record[49]),
                                grab_ent = ParseInt(record[50]),
                                scale_com = ParseInt(record[51]),
                                scale_set = record[52]?.ToString()?.Trim(),
                                scale_code = record[53]?.ToString()?.Trim(),
                                bmp_fondo = record[54]?.ToString()?.Trim(),
                                si_dde = ParseInt(record[55]),
                                serv_dde = record[56]?.ToString()?.Trim(),
                                categdde = record[57]?.ToString()?.Trim(),
                                comandde = record[58]?.ToString()?.Trim(),
                                ocultabar = ParseInt(record[59]),
                                ver_help = ParseInt(record[60]),
                                exe_dde = record[61]?.ToString()?.Trim(),
                                deci_det = ParseInt(record[62]),
                                si_copyto = ParseInt(record[63]),
                                si_dde_2 = ParseInt(record[64]),
                                camara_com = ParseInt(record[65]),
                                egres_act = ParseInt(record[66]),
                                docum_egr = record[67]?.ToString()?.Trim(),
                                datafono = record[68]?.ToString()?.Trim(),
                                asadero = ParseInt(record[69]),
                                fpago_asa = record[70]?.ToString()?.Trim(),
                                sidomicil = ParseInt(record[71]),
                                si_driver = ParseInt(record[72]),
                                tef_port = record[73]?.ToString()?.Trim(),
                                tef_veloc = ParseInt(record[74]),
                                tef_parid = record[75]?.ToString()?.Trim(),
                                tef_bitdat = record[76]?.ToString()?.Trim(),
                                tef_stop = record[77]?.ToString()?.Trim(),
                                tef_buffer = ParseInt(record[78]),
                                tef_time = ParseInt(record[79]),
                                tef_silen = ParseInt(record[80]),
                                tef_exe = record[81]?.ToString()?.Trim(),
                                tef_activo = ParseInt(record[82]),
                                tipo_scale = ParseInt(record[83]),
                                impr_elect = ParseInt(record[84]),
                                num_copias = ParseInt(record[85]),
                                no_act_ccq = ParseInt(record[86]),
                                dat_banco = record[87]?.ToString()?.Trim(),
                                docum_rcj = record[88]?.ToString()?.Trim(),
                                rcj_act = ParseInt(record[89]),
                                erro_pos = ParseInt(record[90]),
                                dat_cnb = record[91]?.ToString()?.Trim(),
                                ver_agota = ParseInt(record[92]),
                                esdomicil = ParseInt(record[93]),
                                tef_solic = record[94]?.ToString()?.Trim(),
                                tef_respu = record[95]?.ToString()?.Trim(),
                                tef_timew = ParseInt(record[96]),
                                tef_dir = record[97]?.ToString()?.Trim(),
                                tip_imp = ParseInt(record[98]),
                                nom_print = record[99]?.ToString()?.Trim(),
                                actqcre = ParseInt(record[100]),
                                usaurano = ParseInt(record[101]),
                                tipobal = ParseInt(record[102]),
                                tef_sicaj = ParseInt(record[103]),
                                tef_vret = record[104]?.ToString()?.Trim(),
                                fopeposl = Convert.ToDateTime(record[105]),
                                mod_teff = ParseInt(record[106]),
                                si_runteff = ParseInt(record[107]),
                                si_esp = ParseInt(record[108]),
                                portcalif = ParseInt(record[109]),
                                ultfact = ParseInt(record[110]),
                                si_parq = ParseInt(record[111]),
                                rut_calif = record[112]?.ToString()?.Trim(),
                                dir_orig = record[113]?.ToString()?.Trim(),
                                dir_dest = record[114]?.ToString()?.Trim(),
                                ip_files = record[115]?.ToString()?.Trim(),
                                ip_servi = record[116]?.ToString()?.Trim(),
                                navegador = record[117]?.ToString()?.Trim(),
                                rcconsult = record[118]?.ToString()?.Trim(),
                                fechaaper = Convert.ToDateTime(record[119]),
                                claveaper = ParseInt(record[120]),
                                modoaper = record[121]?.ToString()?.Trim(),
                                lmte_aper = ParseInt(record[122]),
                                img_seg = ParseInt(record[123]),
                                si_consult = ParseInt(record[124]),
                                cod_tur = record[125]?.ToString()?.Trim(),
                                lim_fesp = ParseInt(record[126]),
                                si_facele = ParseInt(record[127]),
                                docum_fel = record[128]?.ToString()?.Trim(),
                                fele_act = ParseInt(record[129]),
                                si_comanda = ParseInt(record[130]),
                                comprinter = record[131]?.ToString()?.Trim(),
                                facele_seg = ParseInt(record[132]),
                                print_adv = ParseInt(record[133]),
                                si_prompt = ParseInt(record[134]),
                                cuc = record[135]?.ToString()?.Trim(),
                                nuevocred = ParseInt(record[136]),
                                ms2430 = ParseInt(record[137]),
                                vscanner = record[138]?.ToString()?.Trim(),
                                vscale = record[139]?.ToString()?.Trim(),
                                pscanner = record[140]?.ToString()?.Trim(),
                                pscale = record[141]?.ToString()?.Trim(),
                                time_2430 = ParseInt(record[142]),
                                ex_park = ParseInt(record[143]),
                                estan_park = ParseInt(record[144]),
                                si_merma = ParseInt(record[145]),
                                si_conti = ParseInt(record[146]),
                                docum_dc = record[147]?.ToString()?.Trim(),
                                dc_act = ParseInt(record[148]),
                                tef_v924 = ParseInt(record[149]),
                                borrain = ParseInt(record[150]),
                                flujou = record[151]?.ToString()?.Trim(),
                                tiporta = record[152]?.ToString()?.Trim(),
                                previa = record[153]?.ToString()?.Trim(),
                                log_tef = record[154]?.ToString()?.Trim(),
                                escucha = record[155]?.ToString()?.Trim(),
                                provfe = record[156]?.ToString()?.Trim(),
                                fechaprov = Convert.ToDateTime(record[157]),
                            };

                            config5_list.Add(config5);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfConfig))
                {
                    var reader = new DBFReader(fc);
                    //reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var config = new t_config
                            {
                                impbolsa = ParseInt(record[223]),
                                valimpbo = ParseInt(record[224]),
                            };

                            config_list.Add(config);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                await Procesos.InsertarConfigpos(config5_list, config_list, xCodPos);
                await Procesos.InsertarConfigposconsecutivos(config5_list, xCodPos);
                await Procesos.InsertarConfigposotros(config5_list, xCodPos);
                await Procesos.InsertarConfigposteff(config5_list, xCodPos);
                await Procesos.InsertarConfigbalanza(config5_list, xCodPos);
                break;

            case 40:

                using (FileStream fc = File.OpenRead(dbflocaliza))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var localiza = new t_localiza
                            {
                                cod_loc = ParseInt(record[0]),
                                nom_loc = record[1]?.ToString()?.Trim(),
                            };

                            localiza_list.Add(localiza);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }
                await Procesos.InsertarLocalizacion(localiza_list);
                break;

            case 41:

                int xsucursal = 0;
                Console.WriteLine("ingrese la sucursal de las ubicaciones que desea migrar");
                xsucursal = int.Parse(Console.ReadLine());

                using (FileStream fc = File.OpenRead(dbfubicacion))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var ubicacion = new t_ubicacion
                            {
                                cod_ubi = ParseInt(record[0]),
                                nom_ubi = record[1]?.ToString()?.Trim(),
                            };

                            ubicacion_list.Add(ubicacion);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }
                await Procesos.InsertarUbicacion(ubicacion_list, xsucursal);
                break;

            case 42:

                using (FileStream fc = File.OpenRead(dbfcabcaptu))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var cabcaptu = new t_cabcaptu
                            {
                                sucursal = record[0]?.ToString()?.Trim(),
                                cod_cab = ParseInt(record[1]),
                                fecha = Convert.ToDateTime(record[2]),
                                fech_ini = Convert.ToDateTime(record[3]),
                                fech_fin = Convert.ToDateTime(record[4]),
                                hora = record[5]?.ToString()?.Trim(),
                                usuario = record[6]?.ToString()?.Trim(),
                                observac = record[7]?.ToString()?.Trim(),
                                si_pro = ParseInt(record[8]),
                                si_saldo = ParseInt(record[9]),
                                estado = ParseInt(record[10]),
                                cod_unico = record[11]?.ToString()?.Trim(),
                            };

                            cabcaptu_list.Add(cabcaptu);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }
                await Procesos.InsertarIsis(cabcaptu_list);
                break;

            case 43:

                using (FileStream fc = File.OpenRead(dbfdetcaptu))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var detcaptu = new t_detcaptu
                            {
                                cod_captu = ParseInt(record[0]),
                                cod_cab = ParseInt(record[1]),
                                cod_ubi = ParseInt(record[2]),
                                cod_loc = ParseInt(record[3]),
                                nom_det = record[4]?.ToString()?.Trim(),
                                observac = record[5]?.ToString()?.Trim(),
                                fecha = Convert.ToDateTime(record[6]),
                                sucursal = record[7]?.ToString()?.Trim(),
                                cod_unico = record[8]?.ToString()?.Trim(),
                            };

                            detcaptu_list.Add(detcaptu);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfcabcaptu))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var cabcaptu = new t_cabcaptu
                            {
                                sucursal = record[0]?.ToString()?.Trim(),
                                cod_cab = ParseInt(record[1]),
                                fecha = Convert.ToDateTime(record[2]),
                                fech_ini = Convert.ToDateTime(record[3]),
                                fech_fin = Convert.ToDateTime(record[4]),
                                hora = record[5]?.ToString()?.Trim(),
                                usuario = record[6]?.ToString()?.Trim(),
                                observac = record[7]?.ToString()?.Trim(),
                                si_pro = ParseInt(record[8]),
                                si_saldo = ParseInt(record[9]),
                                estado = ParseInt(record[10]),
                                cod_unico = record[11]?.ToString()?.Trim(),
                            };

                            cabcaptu_list.Add(cabcaptu);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbflocaliza))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var localiza = new t_localiza
                            {
                                cod_loc = ParseInt(record[0]),
                                nom_loc = record[1]?.ToString()?.Trim(),
                            };

                            localiza_list.Add(localiza);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfubicacion))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var ubicacion = new t_ubicacion
                            {
                                cod_ubi = ParseInt(record[0]),
                                nom_ubi = record[1]?.ToString()?.Trim(),
                            };

                            ubicacion_list.Add(ubicacion);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                await Procesos.InsertarConteoIsis(detcaptu_list, cabcaptu_list, localiza_list, ubicacion_list);
                break;

            case 44:

                using (FileStream fc = File.OpenRead(dbfusucaptu))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var usucaptu = new t_usucaptu
                            {
                                cod_cab = ParseInt(record[0]),
                                cod_captu = ParseInt(record[1]),
                                usuario = record[2]?.ToString()?.Trim(),
                                conteo = ParseInt(record[3]),
                                coordina = ParseInt(record[4]),
                                estado = ParseInt(record[5]),
                                fecha = Convert.ToDateTime(record[6]),
                                sucursal = record[7]?.ToString()?.Trim(),
                                cod_unico = record[8]?.ToString()?.Trim(),
                            };

                            usucaptu_list.Add(usucaptu);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfdetcaptu))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var detcaptu = new t_detcaptu
                            {
                                cod_captu = ParseInt(record[0]),
                                cod_cab = ParseInt(record[1]),
                                cod_ubi = ParseInt(record[2]),
                                cod_loc = ParseInt(record[3]),
                                nom_det = record[4]?.ToString()?.Trim(),
                                observac = record[5]?.ToString()?.Trim(),
                                fecha = Convert.ToDateTime(record[6]),
                                sucursal = record[7]?.ToString()?.Trim(),
                                cod_unico = record[8]?.ToString()?.Trim(),
                            };

                            detcaptu_list.Add(detcaptu);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfcabcaptu))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var cabcaptu = new t_cabcaptu
                            {
                                sucursal = record[0]?.ToString()?.Trim(),
                                cod_cab = ParseInt(record[1]),
                                fecha = Convert.ToDateTime(record[2]),
                                fech_ini = Convert.ToDateTime(record[3]),
                                fech_fin = Convert.ToDateTime(record[4]),
                                hora = record[5]?.ToString()?.Trim(),
                                usuario = record[6]?.ToString()?.Trim(),
                                observac = record[7]?.ToString()?.Trim(),
                                si_pro = ParseInt(record[8]),
                                si_saldo = ParseInt(record[9]),
                                estado = ParseInt(record[10]),
                                cod_unico = record[11]?.ToString()?.Trim(),
                            };

                            cabcaptu_list.Add(cabcaptu);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbflocaliza))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var localiza = new t_localiza
                            {
                                cod_loc = ParseInt(record[0]),
                                nom_loc = record[1]?.ToString()?.Trim(),
                            };

                            localiza_list.Add(localiza);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                await Procesos.InsertarUsuarioIsis(usucaptu_list, detcaptu_list, cabcaptu_list, localiza_list);
                break;

            case 45:

                using (FileStream fc = File.OpenRead(dbfisiscaptu))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var isiscaptu = new t_isiscaptu
                            {
                                captura = ParseInt(record[0]),
                                cod_captu = ParseInt(record[1]),
                                conteo = ParseInt(record[2]),
                                codigo = record[3]?.ToString()?.Trim(),
                                fecha = Convert.ToDateTime(record[4]),
                                hora = record[5]?.ToString()?.Trim(),
                                usuario = record[6]?.ToString()?.Trim(),
                                cantidad = ParseInt(record[7]),
                                cod_estado = ParseInt(record[8]),
                                sucursal = record[9]?.ToString()?.Trim(),
                                cod_unico = record[10]?.ToString()?.Trim(),
                                cod_cab = ParseInt(record[11]),
                            };

                            isiscaptu_list.Add(isiscaptu);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfdetcaptu))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var detcaptu = new t_detcaptu
                            {
                                cod_captu = ParseInt(record[0]),
                                cod_cab = ParseInt(record[1]),
                                cod_ubi = ParseInt(record[2]),
                                cod_loc = ParseInt(record[3]),
                                nom_det = record[4]?.ToString()?.Trim(),
                                observac = record[5]?.ToString()?.Trim(),
                                fecha = Convert.ToDateTime(record[6]),
                                sucursal = record[7]?.ToString()?.Trim(),
                                cod_unico = record[8]?.ToString()?.Trim(),
                            };

                            detcaptu_list.Add(detcaptu);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfcabcaptu))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var cabcaptu = new t_cabcaptu
                            {
                                sucursal = record[0]?.ToString()?.Trim(),
                                cod_cab = ParseInt(record[1]),
                                fecha = Convert.ToDateTime(record[2]),
                                fech_ini = Convert.ToDateTime(record[3]),
                                fech_fin = Convert.ToDateTime(record[4]),
                                hora = record[5]?.ToString()?.Trim(),
                                usuario = record[6]?.ToString()?.Trim(),
                                observac = record[7]?.ToString()?.Trim(),
                                si_pro = ParseInt(record[8]),
                                si_saldo = ParseInt(record[9]),
                                estado = ParseInt(record[10]),
                                cod_unico = record[11]?.ToString()?.Trim(),
                            };

                            cabcaptu_list.Add(cabcaptu);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbflocaliza))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var localiza = new t_localiza
                            {
                                cod_loc = ParseInt(record[0]),
                                nom_loc = record[1]?.ToString()?.Trim(),
                            };

                            localiza_list.Add(localiza);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfubicacion))
                {
                    var reader = new DBFReader(fc);
                    // reader.CharEncoding = System.Text.Encoding.UTF8;
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252);

                    object[] record;
                    int row = 0; // contador de registros

                    while ((record = reader.NextRecord()) != null)
                    {
                        row++; // incrementa cada vez que lees un registro
                        try
                        {
                            var ubicacion = new t_ubicacion
                            {
                                cod_ubi = ParseInt(record[0]),
                                nom_ubi = record[1]?.ToString()?.Trim(),
                            };

                            ubicacion_list.Add(ubicacion);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error en registro #{row}: {ex.Message}");
                            Console.WriteLine($"   Valores del registro: {string.Join(" | ", record.Select(r => r?.ToString() ?? "NULL"))}");
                            throw; // si quieres parar aquí, o comenta esto si prefieres seguir
                        }
                    }
                }

                await Procesos.InsertarControlInventarioIsis(isiscaptu_list, detcaptu_list, cabcaptu_list, localiza_list, ubicacion_list);
                break;


            case 46:

                using (FileStream fc = File.OpenRead(dbfSeparados))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var separados = new t_separados
                        {
                            terceroid = Convert.ToInt32(record[7]),
                            fechayhoraconsumo = Convert.ToDateTime(record[13]),
                            usuariocreaid = record[4]?.ToString()?.Trim(),
                            fechahoraregistro = Convert.ToDateTime(record[3]),
                            cajasmovimientosid = Convert.ToInt32(record[12]),
                            sucursalid = record[0]?.ToString()?.Trim(),
                            documentofactura = record[11]?.ToString()?.Trim(),
                        };

                        separados_list.Add(separados);
                    }
                }

                Procesos.insertarSeparadosmercanciaabonos(separados_list);

                break;


            case 47: //LibroDiaTesoreriaConceptos catrelina

                using (FileStream fs = File.OpenRead(dbfFileTelibro))
                {
                    var reader = new DBFReader(fs);
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252); // ANSI típico de VFP

                    object[] record;
                    int fila = 0;

                    while (true)
                    {
                        try
                        {
                            record = reader.NextRecord();
                            if (record == null) break;

                            fila++;

                            var telibrot = new t_telibro
                            {
                                fecha = Convert.ToDateTime(record[2]),
                                fpago = record[1]?.ToString().Trim(),
                                cant = Convert.ToInt32(record[4]),
                                //debeLong = Convert.ToInt64(record[5]),
                                //haberLong = Convert.ToInt64(record[6]),
                                concepto = record[16]?.ToString().Trim(),
                                formapago = record[1]?.ToString().Trim(),
                                tipofp = Convert.ToInt32(record[18]),
                                valor = Convert.ToInt64(record[21]),
                                valordef = Convert.ToInt64(record[20]),
                                naturaleza = record[19]?.ToString().Trim(),
                                sucursal = record[8]?.ToString().Trim(),
                                docum = record[9]?.ToString().Trim(),
                                consecut = record[10]?.ToString().Trim(),
                                numero = Convert.ToInt32(record[11]),
                                nit = Convert.ToInt64(record[12]),

                            };

                            telibro_list.Add(telibrot);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error en fila {fila}: {ex.Message}");


                            continue;
                        }
                    }
                }

                await Procesos.InsertarTelibro(telibro_list);
                break;

        }
            
            


            return ;





    }



    private static T GetValueOrDefault<T>(object[] record, int index, T defaultValue)
    {
        if (index < record.Length)
        {
            return record[index] is T value ? value : defaultValue;
        }
        return defaultValue;
    }


    private static decimal ParseCurrency(object value)
    {
        if (value == null || value == DBNull.Value)
            return 0m;

        // Si es un arreglo de bytes (tipo Y en DBF)
        if (value is byte[] bytes)
        {
            // Convierte los 8 bytes a un valor de 64 bits con punto fijo
            long rawValue = BitConverter.ToInt64(bytes, 0);
            return rawValue / 10000m; // FoxPro almacena 4 decimales fijos
        }

        // Si ya viene como número o cadena
        if (decimal.TryParse(value.ToString(), out decimal result))
            return result;

        return 0m;
    }



}


