using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_cliente
    {
        public string tdoc { get; set; }            // Tipo de documento (código)
        public string anexo { get; set; }           // Número de documento
        public string dv { get; set; }              // Dígito de verificación
        public string nombre { get; set; }          // Razón social
        public string direcc { get; set; }          // Dirección
        public string emailfe1 { get; set; }        // Correo electrónico
        public string tel { get; set; }             // Teléfono
        public string apl1 { get; set; }            // Primer apellido
        public string apl2 { get; set; }            // Segundo apellido
        public string nom1 { get; set; }            // Primer nombre
        public string nom2 { get; set; }            // Segundo nombre
        public int bloqueado { get; set; }

        public string Dane { get; set; }


        public string tipo_per { get; set; }
    }
}
