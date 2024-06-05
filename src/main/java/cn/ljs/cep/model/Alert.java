package cn.ljs.cep.model;

public class Alert {
    private String message;

    public Alert(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Alert(message=" + message + ")";
    }
}