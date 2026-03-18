using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_bonodev
    {
        public DateTime fecha { get; set; }
        public string super { get; set; }
        public int num_bono { get; set; }
        public int valorbono { get; set; }
        public int tercero { get; set; }
        public string suc_dev { get; set; }
        public string doc_dev { get; set; }
        public string cons_dev { get; set; }
        public int num_dev { get; set; }
        public string concepto { get; set; }
        public DateTime fech_reg { get; set; }
        public TimeOnly hora_reg { get; set; }
        public string usua_reg { get; set; }
        public string suc_pos { get; set; }
        public string doc_pos { get; set; }
        public DateTime fech_pos { get; set; }
        public string clave { get; set; }
        public int num_pos { get; set; }
        public int estado { get; set; }
        public long terceroLong { get; set; }
    }
}
