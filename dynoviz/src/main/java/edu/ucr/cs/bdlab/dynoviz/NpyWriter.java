package edu.ucr.cs.bdlab.dynoviz;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
public class NpyWriter {

    public static void writeNpyFile(int[] data, int[] shape, String filePath) throws IOException {
        int numComponent = shape[0];
        int numRows = shape[1];
        int numCols = shape[2]; //numRows > 0 ? data[1].length : 0;

        try (FileOutputStream fos = new FileOutputStream(filePath);
             DataOutputStream dos = new DataOutputStream(fos)) {
            // Writing the magic string, major, and minor version numbers
            dos.writeByte(0x93);
            dos.writeBytes("NUMPY");
            dos.writeByte(0x01); // Major version
            dos.writeByte(0x00); // Minor version

            // Placeholder for header length which we will overwrite later
            dos.writeShort(0);

            // Start of header data
            String header = "{'descr': '<u2', 'fortran_order': False, 'shape': (" + numComponent + "," + numRows + "," + numCols + "), }";
            // Calculate padding length for the header to be a multiple of 64 bytes (for alignment)
            int headerLength = header.length() + 1;  // +1 for newline
            int padding = 64 - (headerLength % 64);  // Ensure the header is a multiple of 64 bytes
            headerLength += padding;

            // Add padding spaces to the header
            StringBuilder sb = new StringBuilder(header);
            for (int i = 0; i < padding; i++) {
                sb.append(' ');
            }
            sb.append('\n');  // End with a newline
            header = sb.toString();

            // Go back and write the correct header length
            ByteBuffer buffer = ByteBuffer.allocate(2);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putShort((short) (header.length()));
            byte[] headerLengthBytes = buffer.array();
            fos.getChannel().position(8); // Skip the magic string and version number
            dos.write(headerLengthBytes);

            // Write the actual header
            fos.getChannel().position(10); // Position after magic string, version, and header length
            dos.writeBytes(header);

            // Writing the array data in row-major order (depth, rows, columns)
            ByteBuffer dataBuffer = ByteBuffer.allocate(2);  // 2 bytes for each short
            dataBuffer.order(ByteOrder.LITTLE_ENDIAN);


            int index = 0;
            //while (index < numComponent*numRows*numCols) {
                for (int component = 0; component<numComponent; component+=1){
                    // for each block, write row*col values
                    for(int row = 0; row < numRows; row+=1){
                        for(int col=0; col < numCols; col+=1){
                            buffer.clear();
                            int currValue = data[index];
                            buffer.putShort((short)currValue);
                            dos.write(buffer.array());
                            index+=1;
                            if (index == numComponent*numRows*numCols)
                                break;
                        }
                    }


                }
            //}
            dos.close();
                fos.close();
        }

    }



}
