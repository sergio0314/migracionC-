using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_cabFact
    {
        public DateTime fechahoramovimiento { get; set; }
        public string sucursalid { get; set; }
        public string sucursalnombre { get; set; }
        public int cajaid { get; set; }
        public string cajacodigo { get; set; }
        public string usuarioid { get; set; }
        public string usuarionombre { get; set; }
        public long terceroid { get; set; }
        public string nombretercero { get; set; }
        public long? tarjetamercapesosid { get; set; }
        public string tarjetamercapesoscodigo { get; set; }
        public string documentofactura { get; set; }
        public Boolean anulado { get; set; }
        public string usuarioanulaid { get; set; }
        public string tipotramite { get; set; }
        public int totalventa { get; set; }
        public int valorcambio { get; set; }
        public string tipopagoid { get; set; }
        public int numerofactura { get; set; }
        public string documentoid { get; set; }
        public string documentonombre { get; set; }
        public long? identificacion { get; set; }
        public DateTime fechahoraanula { get; set; }
        public Boolean    transferir { get; set; }

        public string idcaja { get; set; }


    }
}
