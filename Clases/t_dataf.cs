using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_dataf
    {
       public int cajasmovimientosid { get; set; }
       public DateTime fechayhorasolicitud { get; set; }
        public int solicitudvalor { get; set; }
        public int solicitudmonto { get; set; }
        public int solicitudiva { get; set; }
        public int solicitudfactura { get; set; }
        public string solicitudcodigocajero { get; set; }
        public string respuestarespuesta { get; set; }
        public string  respuestaautorizacion { get; set; }
        public string respuestatarjeta { get; set; }
        public string respuestatipotarjeta { get; set; }
        public string respuestafranquicia { get; set; }
        public int respuestamonto { get; set; }
        public int respuestaiva { get; set; }
        public string respuestarecibo { get; set; }
        public string respuestacuotas { get; set; }
        public string respuestarrn { get; set; }
        public string respuestaobservacion { get; set; }

        public string solicitudoperacion { get; set; }

    }
}
