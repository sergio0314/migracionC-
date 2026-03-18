using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_detcaptu
    {
        public int cod_captu { get; set; }
        public int cod_cab { get; set; }
        public int cod_ubi { get; set; }
        public int cod_loc { get; set; }
        public string nom_det { get; set; }
        public string observac { get; set; }
        public DateTime fecha { get; set; }
        public string sucursal { get; set; }
        public string cod_unico { get; set; }
    }
}
