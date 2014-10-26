namespace Microsoft.HBase.Client.UI
{
    partial class TxJSONDerivedForm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.tbMapping = new System.Windows.Forms.TextBox();
            this.btOK = new System.Windows.Forms.Button();
            this.btCancel = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // tbMapping
            // 
            this.tbMapping.Location = new System.Drawing.Point(12, 30);
            this.tbMapping.Multiline = true;
            this.tbMapping.Name = "tbMapping";
            this.tbMapping.Size = new System.Drawing.Size(350, 376);
            this.tbMapping.TabIndex = 0;
            // 
            // btOK
            // 
            this.btOK.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.btOK.Location = new System.Drawing.Point(478, 441);
            this.btOK.Name = "btOK";
            this.btOK.Size = new System.Drawing.Size(105, 23);
            this.btOK.TabIndex = 1;
            this.btOK.Text = "OK";
            this.btOK.UseVisualStyleBackColor = true;
            this.btOK.Click += new System.EventHandler(this.btOK_Click);
            // 
            // btCancel
            // 
            this.btCancel.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.btCancel.Location = new System.Drawing.Point(608, 441);
            this.btCancel.Name = "btCancel";
            this.btCancel.Size = new System.Drawing.Size(105, 23);
            this.btCancel.TabIndex = 2;
            this.btCancel.Text = "Cancel";
            this.btCancel.UseVisualStyleBackColor = true;
            // 
            // TxJSONDerivedForm
            // 
            this.AcceptButton = this.btOK;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.CancelButton = this.btCancel;
            this.ClientSize = new System.Drawing.Size(751, 476);
            this.Controls.Add(this.btCancel);
            this.Controls.Add(this.btOK);
            this.Controls.Add(this.tbMapping);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "TxJSONDerivedForm";
            this.ShowInTaskbar = false;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "TxJSONDerivedForm";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox tbMapping;
        private System.Windows.Forms.Button btOK;
        private System.Windows.Forms.Button btCancel;
    }
}