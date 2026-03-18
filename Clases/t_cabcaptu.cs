using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_cabcaptu
    {
        public string sucursal { get; set; }
        public int cod_cab { get; set; }
        public DateTime fecha { get; set; }
        public DateTime fech_ini { get; set; }
        public DateTime fech_fin { get; set; }
        public string hora { get; set; }
        public string usuario { get; set; }
        public string observac { get; set; }
        public int si_pro { get; set; }
        public int si_saldo { get; set; }
        public int estado { get; set; }
        public string cod_unico { get; set; }
    }
}
