using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_cuotas
    {

        public int fenalpagid { get; set; }
        public string cuotanumero { get; set; }
        public int valorcuota { get; set; }
        public DateTime fechavencimientocuota { get; set; }
        public DateTime fechahorapago { get; set; }
        public string idusuariopago { get; set; }
        public int cajasmovimientosid { get; set; }
        public int valorabono { get; set; }
        public string sucursalid { get; set; }
        public string documentofactura { get; set; }


    }
}
