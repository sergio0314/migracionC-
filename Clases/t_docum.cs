using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    class t_docum
    {
        public string docum { get; set; }//codigo
        public string nombre { get; set; }//nombre
        public int tipo_doc { get; set; }//asimilar
        public int contabil { get; set; }//CD
        public int si_cnomb { get; set; }//CT
        public int bloqueado { get; set; }//IN
        public int vali_doc { get; set; }//VD
        public int si_consec { get; set; }//UV
        public int controlrut { get; set; }//CR
        public int camb_ter { get; set; }//PC
        public int desc_ord { get; set; }//DO
        public int es_trans { get; set; }//TR
        public int cons_proc { get; set; }//CA
        public int desc_doci { get; set; }//DD
        public int silibtes { get; set; }//LT
        public int n_lineas { get; set; }//NL, esta campo para nosotros es numerico, ellos lo tienen boleano
        public int n_recup { get; set; }//RD
        public int obser_doc { get; set; }//RO
        public int cont_fec { get; set; }//ControlFechas
        public int vend_det { get; set; }//Vendedor
        public int zon_det { get; set; }//Zona
        public int cco_det { get; set; }//CCosto
        public int es_resolu { get; set; } //Resolucion
        public int sniif_on { get; set; }//ActivarColumna
        public int si_contpag { get; set; }//ControlaPagos
        public DateTime fecha_cre { get; set; }//FechaCreacion
        public string Mensaje1 { get; set; }//Mensaje1
        public string Mensaje2 { get; set; }//Mensaje2
        public string Mensaje3 { get; set; }//Mensaje3
        public int afin_cxc { get; set; }//ValoresCartera
        public string Anexo1 { get; set; }//Anexo1
        public string Anexo2 { get; set; }//Anexo2
        public string Anexo3 { get; set; }//Anexo3
        public string Anexo4 { get; set; }//Anexo4
        public string Anexo5 { get; set; }//Anexo5
        public string Anexo6 { get; set; }//Anexo6
        public int afin_tipo { get; set; }//MovimientoCartera
        public string afin_doc { get; set; }//FusionarDocumento

    }
}
