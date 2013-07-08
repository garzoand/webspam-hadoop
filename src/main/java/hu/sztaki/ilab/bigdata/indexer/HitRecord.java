package hu.sztaki.ilab.bigdata.indexer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class HitRecord implements Writable {

    public static class Hit implements WritableComparable<Hit>{

        private byte fieldType;
        private int fieldPos;

        public Hit() {
            
        }
       
        public Hit(byte fieldType, int fieldPos) {
            this.fieldType = fieldType;
            this.fieldPos = fieldPos;
        }

        public byte getFieldType() {
            return fieldType;
        }

        public void setFieldType(byte fieldType) {
            this.fieldType = fieldType;
        }

        public int getFieldPos() {
            return fieldPos;
        }

        public void setFieldPos(int fieldPos) {
            this.fieldPos = fieldPos;
        }
        
        public void write(DataOutput out) throws IOException {
            out.write(fieldType);
            out.writeInt(fieldPos);
        }
        
        public void readFields(DataInput in) throws IOException {
            fieldType = in.readByte();
            fieldPos = in.readInt();
        }
        
        @Override
        public String toString() {
            return "Hit [fieldType=" + Byte.toString(fieldType) + ", fieldPos=" + fieldPos + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + fieldPos;
            result = prime * result + fieldType;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Hit other = (Hit) obj;
            if (fieldPos != other.fieldPos) {
                return false;
            }
            if (fieldType != other.fieldType) {
                return false;
            }
            return true;
        }

        public int compareTo(Hit o) {
            
            int result = new Byte(fieldType).compareTo(new Byte(o.fieldType));
            if (result != 0) {
                return result;
            }
           
            if (fieldPos < o.getFieldPos()) {
                return result = -1;
            }
            if (fieldPos > o.getFieldPos()) {
                return result = 1;
            }
            
            return result;
            
        }
        
    }

    private Set<Hit> hits = new TreeSet<Hit>();
    
    public void addHit(Hit hit) {
        hits.add(hit);
    }
    
    public Set<Hit> getHits() {
        return hits;
    }
    
    public void write(DataOutput out) throws IOException {
        out.writeLong(hits.size());
        for (Hit hit : hits) {
            hit.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        long size = in.readLong();
        hits = new TreeSet<HitRecord.Hit>();
        for (int i = 0; i < size; i++) {
            Hit hit = new Hit();
            hit.readFields(in);
            hits.add(hit);
        }
    }

    @Override
    public String toString() {
        return hits.toString();
        
    }
    
}
