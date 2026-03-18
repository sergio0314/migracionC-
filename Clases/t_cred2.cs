using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_cred2
    {
        public string documentoreferencia { get; set; }
        public DateTime fechahoracredito { get; set; }
        public string sucursalid { get; set; }
        public string numerocuenta { get; set; }
        public string documentoaf { get; set; }
        public string tipoabono { get; set; }
        public int totalcredito { get; set; }
        public string consecutivoaf { get; set; }
        public string cuotafija { get; set; }
        public string amortizado { get; set; }
    }
}
